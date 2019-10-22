use std::{
    io::{self, Write},
    error::Error,
    collections::{HashMap, hash_map, VecDeque, BinaryHeap},
    sync::{Arc, Mutex, Condvar},
    cell::RefCell,
    time::{Duration, Instant},
};

use byteorder::ByteOrder;
use crossbeam_channel::{self as channel, Sender, Receiver};

pub type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct NodeId(pub u64);

impl NodeId {
    const UNKNOWN: NodeId = NodeId(0);
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "NodeId({:#x})", self.0)
    }
}

pub struct Message {
    pub peer_id: NodeId,
    pub payload: Vec<u8>,
}

pub struct NetworkNode {
    thread: Option<std::thread::JoinHandle<()>>,

    msgs_in_recver: Receiver<Message>,

    peer_id2output: Mutex<HashMap<NodeId, Arc<PeerOutput>>>,
    output_reqs_sender: Option<Sender<Arc<PeerOutput>>>,

    notify_readiness: Mutex<mio::SetReadiness>,
}

impl NetworkNode {
    const OUT_BUF_CAPACITY: usize = 4096; // May overflow if we will need to send a big message.

    pub fn bind(my_id: NodeId) -> Result<NetworkNode> {
        let my_addr = NODE_TO_ADDR.get(&my_id).ok_or(format!("couldn't find my id: {}", my_id))?;
        eprintln!("node {}: will bind to addr: {}", my_id, my_addr);

        let listener = mio::net::TcpListener::bind(my_addr)?;
        let poll = mio::Poll::new()?;

        poll.register(
            &listener,
            ThreadPriv::LISTENER_TOKEN,
            mio::Ready::readable(),
            mio::PollOpt::edge(),
        )?;

        let (_notify_registration, notify_readiness) = mio::Registration::new2();
        poll.register(
            &_notify_registration,
            ThreadPriv::NOTIFY_TOKEN,
            mio::Ready::readable(),
            mio::PollOpt::edge(),
        )?;

        let (msgs_in_sender, msgs_in_recver) = channel::unbounded();
        let (output_reqs_sender, output_reqs_recver) = channel::unbounded();

        let thread = std::thread::spawn(move || {
            let mut thread_priv = ThreadPriv {
                my_id,

                poll,
                listener,
                channels: slab::Slab::with_capacity(1024),

                ready_tokens: VecDeque::with_capacity(1024),
                timers: BinaryHeap::new(),

                msgs_in_sender,
                output_reqs_recver,

                _notify_registration,
            };
            let mut peer_id2channel = HashMap::new();
            thread_priv.work(&mut peer_id2channel).unwrap();
        });

        Ok(NetworkNode {
            thread: Some(thread),

            msgs_in_recver,

            peer_id2output: Default::default(),
            output_reqs_sender: Some(output_reqs_sender),
            notify_readiness: Mutex::new(notify_readiness),
        })
    }

    pub fn send(&self, peer_id: NodeId, payload: &[u8], timeout: Option<Duration>) -> Result<()> {
        let deadline = timeout.map(|t| Instant::now() + t);

        let peer = {
            let mut peer_id2output = self.peer_id2output.lock().unwrap();
            peer_id2output
                .entry(peer_id)
                .or_insert_with(|| {
                    Arc::new(PeerOutput {
                        id: peer_id,
                        out_buf: Mutex::new(OutputBuf::with_capacity(Self::OUT_BUF_CAPACITY)),
                        out_cond: Condvar::new(),
                    })
                })
                .clone()
        };

        {
            let mut out_buf = peer.out_buf.lock().unwrap();
            loop {
                match out_buf.push_msg(&payload) {
                    Ok(()) => break,
                    Err(e) if e.kind() != io::ErrorKind::WouldBlock => return Err(e.into()),
                    _ => {}
                }

                out_buf = if let Some(deadline) = deadline {
                    let now = Instant::now();
                    let cur_timeout = if now < deadline {
                        deadline - now
                    } else {
                        return Err(
                            io::Error::new(io::ErrorKind::TimedOut, "send timed out").into()
                        );
                    };
                    peer.out_cond.wait_timeout(out_buf, cur_timeout).unwrap().0
                } else {
                    peer.out_cond.wait(out_buf).unwrap()
                }
            }
        }

        self.output_reqs_sender.as_ref().unwrap().send(peer).unwrap();
        self.notify_readiness.lock().unwrap().set_readiness(mio::Ready::readable()).unwrap();
        Ok(())
    }

    pub fn flush(&self) {
        let peer_id2output = self.peer_id2output.lock().unwrap();
        for (_, peer) in peer_id2output.iter() {
            let mut out_buf = peer.out_buf.lock().unwrap();
            while !out_buf.is_done() {
                out_buf = peer.out_cond.wait(out_buf).unwrap();
            }
        }
    }

    pub fn recv(&self, timeout: Option<Duration>) -> Result<Message> {
        let msg = if let Some(timeout) = timeout {
            self.msgs_in_recver.recv_timeout(timeout)?
        } else {
            self.msgs_in_recver.recv()?
        };

        Ok(msg)
    }
}

impl Drop for NetworkNode {
    fn drop(&mut self) {
        self.output_reqs_sender.take();
        self.notify_readiness.get_mut().unwrap().set_readiness(mio::Ready::readable()).unwrap();
        self.thread.take().unwrap().join().unwrap();
    }
}

lazy_static::lazy_static! {
    static ref NODE_TO_ADDR: HashMap<NodeId, std::net::SocketAddr> = [
        (NodeId(0xdeadbeef), "127.0.0.1:2000"),
        (NodeId(0xcafebabe), "127.0.0.1:2001"),
        (NodeId(0xfeedface), "127.0.0.1:2002"),
    ].iter().map(|(id, addr)| (id.clone(), addr.parse().unwrap())).collect();
}

#[derive(Debug)]
enum MessageHeader {
    Greeting,
    Data { len: usize },
}

impl MessageHeader {
    const GREETING: u32 = 0x80000000;
    const DATA: u32 = 0x00000000;
    const LENGTH_MASK: u32 = 0x00ffffff;

    fn serialize(&self) -> io::Result<u32> {
        match self {
            MessageHeader::Greeting => Ok(MessageHeader::GREETING | 16),
            MessageHeader::Data { len } => {
                if *len <= MessageHeader::LENGTH_MASK as usize {
                    Ok((*len as u32) | MessageHeader::DATA)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("msg length {} is too big", len),
                    ))
                }
            }
        }
    }

    fn deserialize(serialized: u32) -> io::Result<Self> {
        let len: u32 = serialized & MessageHeader::LENGTH_MASK;
        match serialized - len {
            MessageHeader::GREETING => Ok(MessageHeader::Greeting),
            MessageHeader::DATA => Ok(MessageHeader::Data { len: len as usize }),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad msg header: {:#x}", serialized),
            )),
        }
    }
}

/// A buffer of fixed length in the process of reading it from a socket.
enum ReadingBuf<Buf: AsRef<[u8]> + AsMut<[u8]>> {
    Reading { buf: Buf, nread: usize },
    Done(Buf),
}

impl<Buf: AsRef<[u8]> + AsMut<[u8]>> std::fmt::Debug for ReadingBuf<Buf> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reading { buf, nread } => write!(
                f,
                "ReadingBuf::Reading {{ buf.len: {}, nread: {} }}",
                buf.as_ref().len(),
                nread
            ),
            Self::Done(buf) => write!(f, "ReadingBuf::Done {{ buf.len: {} }}", buf.as_ref().len()),
        }
    }
}

impl<Buf: AsRef<[u8]> + AsMut<[u8]> + Default> ReadingBuf<Buf> {
    fn new(buf: Buf) -> Self {
        ReadingBuf::Reading { buf, nread: 0 }
    }

    fn read<T: io::Read>(&mut self, reader: &mut T) -> io::Result<()> {
        if let Self::Reading { ref mut buf, ref mut nread } = self {
            let n = reader.read(&mut buf.as_mut()[*nread..])?;
            if n == 0 {
                return Err(io::ErrorKind::UnexpectedEof.into());
            }

            *nread += n;

            if *nread == buf.as_ref().len() {
                let buf = std::mem::replace(buf, Buf::default());
                *self = Self::Done(buf);
            }
        }

        Ok(())
    }
}

enum ReadingMsg {
    ReadingHeader { buf: ReadingBuf<[u8; 4]> },
    DoneHeader(MessageHeader),

    ReadingData { buf: ReadingBuf<Vec<u8>> },
    DoneData(Vec<u8>),

    ReadingGreeting { buf: ReadingBuf<[u8; 16]> },
    DoneGreeting([u8; 16]),
}

impl std::fmt::Debug for ReadingMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadingHeader { buf } => write!(f, "ReadingHeader({:?})", buf),
            Self::DoneHeader(..) => write!(f, "DoneHeader"),
            Self::ReadingData { buf } => write!(f, "ReadingData({:?})", buf),
            Self::DoneData(buf) => write!(f, "DoneData(buf.len: {})", buf.len()),
            Self::ReadingGreeting { buf } => write!(f, "ReadingGreeting({:?})", buf),
            Self::DoneGreeting(..) => write!(f, "DoneGreeting"),
        }
    }
}

impl ReadingMsg {
    fn new() -> Self {
        ReadingMsg::ReadingHeader { buf: ReadingBuf::new([0u8; 4]) }
    }

    fn read<T: io::Read>(&mut self, reader: &mut T) -> io::Result<()> {
        match self {
            Self::ReadingHeader { ref mut buf } => {
                buf.read(reader)?;
                if let ReadingBuf::Done(header_buf) = buf {
                    let header_u32 = byteorder::NetworkEndian::read_u32(header_buf);
                    let header = MessageHeader::deserialize(header_u32)?;
                    *self = Self::DoneHeader(header);
                }
            }

            Self::DoneHeader(header) => {
                match header {
                    MessageHeader::Greeting => {
                        *self = ReadingMsg::ReadingGreeting { buf: ReadingBuf::new([0u8; 16]) };
                    }

                    MessageHeader::Data { len } => {
                        *self = ReadingMsg::ReadingData { buf: ReadingBuf::new(vec![0u8; *len]) };
                    }
                };

                self.read(reader)?;
            }

            Self::ReadingData { ref mut buf } => {
                buf.read(reader)?;
                if let ReadingBuf::Done(payload_buf) = buf {
                    let payload_buf = std::mem::replace(payload_buf, vec![]);
                    *self = Self::DoneData(payload_buf);
                }
            }

            Self::ReadingGreeting { ref mut buf } => {
                buf.read(reader)?;
                if let ReadingBuf::Done(payload_buf) = buf {
                    let payload_buf = std::mem::replace(payload_buf, [0u8; 16]);
                    *self = Self::DoneGreeting(payload_buf);
                }
            }

            _ => {}
        };

        Ok(())
    }

    fn is_done(&self) -> bool {
        if let Self::DoneData(_) | Self::DoneGreeting(_) = self {
            true
        } else {
            false
        }
    }
}

struct OutputBuf {
    buf: Vec<u8>,
    written: usize,
    sizes: VecDeque<usize>,
    msg_start: usize,
}

impl OutputBuf {
    fn with_capacity(cap: usize) -> Self {
        Self { buf: Vec::with_capacity(cap), written: 0, sizes: VecDeque::new(), msg_start: 0 }
    }

    fn is_done(&self) -> bool {
        self.written == self.buf.len()
    }

    fn sync_after_error(&mut self) {
        self.written = self.msg_start;
    }

    fn prepare_slice(&mut self, len: usize) -> io::Result<&mut [u8]> {
        if self.is_done() {
            self.buf.clear();
            self.written = 0;
            self.sizes.clear();
            self.msg_start = 0;
        }

        if self.written > 0 {
            // Do not allow writing to the buf if the previous writes have not been flushed.
            // I'm not sure about this one, but keeping the buf full is bad for latency.
            return Err(io::ErrorKind::WouldBlock.into());
        }

        if self.buf.len() != 0 && len + self.buf.len() > self.buf.capacity() {
            return Err(io::ErrorKind::WouldBlock.into());
            // If by contrast buf.len() == 0 this means that the message is big
            // and we have no choice but to increase buf capacity to accomodate it.
        }

        let prev_len = self.buf.len();
        self.buf.resize(prev_len + len, 0u8);
        self.sizes.push_back(len);
        Ok(&mut self.buf[prev_len..])
    }

    fn push_msg(&mut self, data: &[u8]) -> io::Result<()> {
        let header: u32 = MessageHeader::Data { len: data.len() }.serialize()?;
        let slice = self.prepare_slice(4 + data.len())?;
        byteorder::NetworkEndian::write_u32(slice, header);
        slice[4..].copy_from_slice(data);
        Ok(())
    }

    fn flush_until_blocked(&mut self, sock: &mio::tcp::TcpStream) -> io::Result<()> {
        while self.written != self.buf.len() {
            match (&*sock).write(&self.buf[self.written..]) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(mut n) => {
                    self.written += n;
                    while let Some(size) = self.sizes.front() {
                        if n < *size {
                            break;
                        }

                        self.msg_start += size;
                        n -= size;
                        self.sizes.pop_front();
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }
}

type SockPtr = Arc<RefCell<mio::net::TcpStream>>;

struct ReadHandle {
    sock: SockPtr,
}

impl io::Read for ReadHandle {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.sock.borrow_mut().read(buf)
    }
}

struct WriteHandle {
    sock: SockPtr,
}

impl io::Write for WriteHandle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.sock.borrow_mut().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.sock.borrow_mut().flush()
    }
}

#[derive(Debug)]
enum ChannelState {
    AcceptedWaitingGreeting,
    AcceptedSendingGreeting,

    ConnectingSendingGreeting { n_attempts: usize },
    ConnectingWaitingGreeting,

    Established,
    ReadClosed,
}

impl ChannelState {
    fn is_greeting_expected(&self) -> bool {
        if let ChannelState::AcceptedWaitingGreeting | ChannelState::ConnectingWaitingGreeting =
            self
        {
            true
        } else {
            false
        }
    }

    fn is_data_expected(&self) -> bool {
        if let ChannelState::Established = self {
            true
        } else {
            false
        }
    }
}

struct Channel {
    state: ChannelState,

    slab_key: usize,
    readiness: mio::Ready,
    is_scheduled: bool,

    sock: SockPtr,
    buf_read: io::BufReader<ReadHandle>,

    my_id: NodeId,
    peer_id: NodeId,

    cur_msg_in: ReadingMsg,

    greeting_msg_out: [u8; 20],
    greeting_msg_out_written: usize,

    output: Option<Arc<PeerOutput>>,
}

#[derive(Debug)]
enum Poll {
    Yielded,
    Blocked,
    Finished,
}

impl Channel {
    const MAX_MSGS_IN: usize = 4096;

    fn connecting(my_id: NodeId, peer_id: NodeId, sock: SockPtr, slab_key: usize) -> Self {
        let mut ret = Self::new(
            ChannelState::ConnectingSendingGreeting { n_attempts: 0 },
            my_id,
            peer_id,
            sock,
            slab_key,
        );
        ret.prepare_greeting_msg();
        ret
    }

    fn accepting(my_id: NodeId, sock: SockPtr, slab_key: usize) -> Self {
        Self::new(ChannelState::AcceptedWaitingGreeting, my_id, NodeId::UNKNOWN, sock, slab_key)
    }

    fn new(
        state: ChannelState,
        my_id: NodeId,
        peer_id: NodeId,
        sock: SockPtr,
        slab_key: usize,
    ) -> Self {
        assert!(my_id != NodeId::UNKNOWN);

        Self {
            state,

            slab_key,
            readiness: mio::Ready::readable() | mio::Ready::writable(),
            is_scheduled: false,

            sock: sock.clone(),

            buf_read: io::BufReader::new(ReadHandle { sock }),

            my_id,
            peer_id,

            cur_msg_in: ReadingMsg::new(),

            greeting_msg_out: [0u8; 20],
            greeting_msg_out_written: 0,

            output: None,
        }
    }

    fn reconnect(&mut self, sock: SockPtr) {
        assert_ne!(self.peer_id, NodeId::UNKNOWN);
        let n_attempts = match self.state {
            ChannelState::ConnectingSendingGreeting { n_attempts } => n_attempts + 1,
            _ => 0,
        };
        let new_state = ChannelState::ConnectingSendingGreeting { n_attempts };
        *self = Self::new(new_state, self.my_id, self.peer_id, sock, self.slab_key);
        self.prepare_greeting_msg();
    }

    /// Return Poll::Blocked if the read edge was cleared
    fn read(
        &mut self,
        this_ptr: &ChannelPtr,
        peer_id2channel: &mut HashMap<NodeId, ChannelPtr>,
        msg_sender: &Sender<Message>,
    ) -> Result<Poll> {
        if !self.readiness.is_readable() {
            return Ok(Poll::Blocked);
        }

        if let ChannelState::ReadClosed = self.state {
            return Ok(Poll::Finished);
        }

        let mut nread = 0;
        loop {
            if let Err(e) = self.cur_msg_in.read(&mut self.buf_read) {
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                } else if e.kind() == io::ErrorKind::WouldBlock {
                    self.readiness &= !mio::Ready::readable();
                    return Ok(Poll::Blocked);
                } else if e.kind() == io::ErrorKind::UnexpectedEof {
                    if !self.cur_msg_in.is_done() {
                        eprintln!("node {}: peer {} suddenly went away", self.my_id, self.peer_id);
                        return Err(e.into());
                    }

                    self.state = ChannelState::ReadClosed;
                    return Ok(Poll::Finished);
                } else {
                    return Err(e.into());
                }
            }

            match self.cur_msg_in {
                ReadingMsg::DoneHeader(ref header @ MessageHeader::Greeting)
                    if !self.state.is_greeting_expected() =>
                {
                    return Err(format!("unexpected {:?} msg", header).into());
                }

                ReadingMsg::DoneHeader(ref header @ MessageHeader::Data { .. }) => {
                    if !self.state.is_data_expected() {
                        return Err(format!("unexpected {:?} msg", header).into());
                    }

                    if nread + 1 >= Self::MAX_MSGS_IN {
                        return Ok(Poll::Yielded);
                    }
                }

                ReadingMsg::DoneGreeting(payload) => {
                    self.identify(&payload)?;
                    peer_id2channel.entry(self.peer_id).or_insert_with(|| this_ptr.clone());

                    self.state = match self.state {
                        ChannelState::AcceptedWaitingGreeting => {
                            ChannelState::AcceptedSendingGreeting
                        }
                        ChannelState::ConnectingWaitingGreeting => ChannelState::Established,
                        _ => unreachable!(),
                    };

                    self.cur_msg_in = ReadingMsg::new();
                    return Ok(Poll::Yielded);
                }

                ReadingMsg::DoneData(ref mut payload) => {
                    let payload = std::mem::replace(payload, vec![]);
                    msg_sender.send(Message { peer_id: self.peer_id, payload }).unwrap();
                    nread += 1;
                    self.cur_msg_in = ReadingMsg::new();
                }

                _ => {}
            }
        }
    }

    fn prepare_greeting_msg(&mut self) {
        let header = MessageHeader::Greeting.serialize().unwrap();
        byteorder::NetworkEndian::write_u32(&mut self.greeting_msg_out[..4], header);
        byteorder::NetworkEndian::write_u64(&mut self.greeting_msg_out[4..12], self.my_id.0);
        byteorder::NetworkEndian::write_u64(&mut self.greeting_msg_out[12..20], self.peer_id.0);
    }

    fn write(&mut self) -> io::Result<Poll> {
        if !self.readiness.is_writable() {
            return Ok(Poll::Blocked);
        }

        let res: io::Result<Poll> = (|| {
            let sock = self.sock.borrow();
            match self.state {
                ChannelState::AcceptedWaitingGreeting | ChannelState::ConnectingWaitingGreeting => {
                    Ok(Poll::Blocked)
                }

                ChannelState::AcceptedSendingGreeting
                | ChannelState::ConnectingSendingGreeting { .. } => {
                    let n =
                        (&*sock).write(&self.greeting_msg_out[self.greeting_msg_out_written..])?;
                    self.greeting_msg_out_written += n;

                    self.state = if let ChannelState::AcceptedSendingGreeting = self.state {
                        ChannelState::Established
                    } else {
                        ChannelState::ConnectingWaitingGreeting
                    };

                    Ok(Poll::Yielded)
                }

                ChannelState::Established | ChannelState::ReadClosed => {
                    if let Some(ref mut output) = self.output {
                        let mut buf = output.out_buf.lock().unwrap();
                        buf.flush_until_blocked(&sock)?;
                        if buf.is_done() {
                            output.out_cond.notify_one();
                        }
                    }

                    Ok(Poll::Blocked)
                }
            }
        })();

        if let Err(e) = res {
            if e.kind() == io::ErrorKind::WouldBlock {
                self.readiness &= !mio::Ready::writable();
                Ok(Poll::Blocked)
            } else {
                Err(e)
            }
        } else {
            res
        }
    }

    fn identify(&mut self, greeting_payload: &[u8; 16]) -> Result<()> {
        assert!(self.state.is_greeting_expected());

        let greeting_my_id = NodeId(byteorder::NetworkEndian::read_u64(&greeting_payload[0..8]));
        let greeting_peer_id = NodeId(byteorder::NetworkEndian::read_u64(&greeting_payload[8..16]));

        if greeting_my_id == NodeId::UNKNOWN {
            return Err(format!("peer addr: {} doesn't know its id", self.peer_id).into());
        }

        if greeting_peer_id != NodeId::UNKNOWN && greeting_peer_id != self.my_id {
            return Err(format!(
                "our id is: {}, but peer {} expected it to be {}",
                self.my_id, greeting_my_id, greeting_peer_id
            )
            .into());
        }

        if self.peer_id == NodeId::UNKNOWN {
            self.peer_id = greeting_my_id;
            self.prepare_greeting_msg();
        } else if self.peer_id != greeting_my_id {
            return Err(
                format!("expected peer id: {}, but got: {}", self.peer_id, greeting_my_id).into()
            );
        }

        eprintln!("node {}: identified channel with peer: {}!", self.my_id, self.peer_id);
        Ok(())
    }
}

type ChannelPtr = Arc<RefCell<Channel>>;

struct PeerOutput {
    id: NodeId,
    out_buf: Mutex<OutputBuf>,
    out_cond: Condvar,
}

struct ThreadPriv {
    my_id: NodeId,

    poll: mio::Poll,
    listener: mio::net::TcpListener,
    channels: slab::Slab<ChannelPtr>,

    ready_tokens: VecDeque<mio::Token>,
    timers: BinaryHeap<Timer>,

    msgs_in_sender: Sender<Message>,
    output_reqs_recver: Receiver<Arc<PeerOutput>>,

    _notify_registration: mio::Registration,
}

struct Timer {
    deadline: Instant,
    callback: Box<dyn FnOnce(&mut ThreadPriv) -> Result<()>>,
}

impl PartialEq for Timer {
    fn eq(&self, rhs: &Self) -> bool {
        self.deadline == rhs.deadline
    }
}

impl Eq for Timer {}

impl PartialOrd for Timer {
    fn partial_cmp(&self, rhs: &Self) -> Option<std::cmp::Ordering> {
        self.deadline.partial_cmp(&rhs.deadline).map(|c| c.reverse())
    }
}

impl Ord for Timer {
    fn cmp(&self, rhs: &Self) -> std::cmp::Ordering {
        self.deadline.cmp(&rhs.deadline).reverse()
    }
}

impl ThreadPriv {
    const LISTENER_TOKEN: mio::Token = mio::Token(1);
    const NOTIFY_TOKEN: mio::Token = mio::Token(2);

    fn is_channel_token(t: mio::Token) -> bool {
        t.0 >= (2 << 16)
    }
    fn token_to_slab_key(t: mio::Token) -> usize {
        t.0 - (2 << 16)
    }
    fn slab_key_to_token(k: usize) -> mio::Token {
        mio::Token(k + (2 << 16))
    }

    fn work(&mut self, peer_id2channel: &mut HashMap<NodeId, ChannelPtr>) -> Result<()> {
        let mut events = mio::Events::with_capacity(1024);
        loop {
            let timeout = loop {
                if let Some(timer) = self.timers.peek() {
                    let now = Instant::now();
                    if timer.deadline > now {
                        break Some(timer.deadline.duration_since(now));
                    }
                } else {
                    break None;
                }

                let Timer { callback, .. } = self.timers.pop().unwrap();
                callback(self)?;
            };

            if self.ready_tokens.is_empty() {
                self.poll.poll(&mut events, timeout)?;
                if events.is_empty() {
                    // it is a timeout, let's fire some timers!
                    continue;
                }

                for event in &events {
                    if Self::is_channel_token(event.token()) {
                        let channel_ptr = self.get_channel(event.token());
                        let mut channel = channel_ptr.borrow_mut();
                        channel.readiness |= event.readiness();
                        self.schedule_channel(&mut *channel);
                    } else {
                        self.ready_tokens.push_back(event.token());
                    }
                }
            }

            if let Some(token) = self.ready_tokens.pop_front() {
                if !self.dispatch(peer_id2channel, token)? {
                    eprintln!("node {}: finished background thread", self.my_id);
                    return Ok(());
                }
            }
        }
    }

    fn schedule_channel(&mut self, channel: &mut Channel) {
        if channel.is_scheduled {
            return;
        } else {
            channel.is_scheduled = true;
        }

        let token = Self::slab_key_to_token(channel.slab_key);
        match channel.state {
            ChannelState::ConnectingSendingGreeting { n_attempts } if n_attempts > 0 => {
                self.timers.push(Timer {
                    deadline: Instant::now() + Duration::from_secs(2), // XXX: hardcode
                    callback: Box::new(move |this: &mut ThreadPriv| {
                        this.ready_tokens.push_back(token);
                        Ok(())
                    }),
                });
            }

            _ => {
                self.ready_tokens.push_back(token);
            }
        }
    }

    /// false if the thread must stop
    fn dispatch(
        &mut self,
        peer_id2channel: &mut HashMap<NodeId, ChannelPtr>,
        token: mio::Token,
    ) -> Result<bool> {
        if token == Self::LISTENER_TOKEN {
            match self.listener.accept() {
                Ok((sock, addr)) => {
                    eprintln!("node {}: connection from {}!", self.my_id, addr);

                    let entry = self.channels.vacant_entry();
                    let slab_key = entry.key();
                    let sock_ptr = Arc::new(RefCell::new(sock));
                    self.poll.register(
                        &*sock_ptr.borrow(),
                        Self::slab_key_to_token(slab_key),
                        mio::Ready::readable() | mio::Ready::writable(),
                        mio::PollOpt::edge(),
                    )?;
                    let channel_ptr =
                        Arc::new(RefCell::new(Channel::accepting(self.my_id, sock_ptr, slab_key)));
                    entry.insert(channel_ptr);
                }

                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(true),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e.into()),
            }

            self.ready_tokens.push_back(token);
        } else if token == Self::NOTIFY_TOKEN {
            loop {
                let peer_output = match self.output_reqs_recver.try_recv() {
                    Ok(p) => p,
                    Err(channel::TryRecvError::Empty) => break,
                    Err(channel::TryRecvError::Disconnected) => return Ok(false),
                };

                let channel_ptr = match peer_id2channel.entry(peer_output.id) {
                    hash_map::Entry::Occupied(oe) => oe.into_mut(),
                    hash_map::Entry::Vacant(ve) => ve.insert(self.open_channel(peer_output.id)?),
                };

                let mut channel = channel_ptr.borrow_mut();
                channel.output.replace(peer_output);
                self.schedule_channel(&mut channel);
            }
        } else {
            let channel_ptr = self.get_channel(token);
            if let Err(e) = self.dispatch_channel(peer_id2channel, token, &channel_ptr) {
                let mut channel = channel_ptr.borrow_mut();
                eprintln!("node {}: channel to peer {}: error: {}", self.my_id, channel.peer_id, e);
                if let Some(output) = channel.output.take() {
                    let mut buf = output.out_buf.lock().unwrap();
                    if buf.is_done() {
                        self.close_channel(&mut *channel, &channel_ptr, peer_id2channel);
                    } else {
                        buf.sync_after_error();
                        drop(buf);

                        eprintln!(
                            "node {}: channel to peer {}: some output left, reconnecting",
                            self.my_id, channel.peer_id
                        );
                        self.try_reconnect_channel(&mut *channel, output).unwrap();
                    }
                } else {
                    self.close_channel(&mut *channel, &channel_ptr, peer_id2channel);
                }
            }
        }

        Ok(true)
    }

    fn dispatch_channel(
        &mut self,
        peer_id2channel: &mut HashMap<NodeId, ChannelPtr>,
        token: mio::Token,
        channel_ptr: &ChannelPtr,
    ) -> Result<()> {
        let mut channel = channel_ptr.borrow_mut();
        channel.is_scheduled = false;

        let mut reschedule = false;
        if let Poll::Yielded = channel.write()? {
            reschedule = true;
        }

        if let Poll::Yielded = channel.read(&channel_ptr, peer_id2channel, &self.msgs_in_sender)? {
            reschedule = true;
        }

        if reschedule {
            self.ready_tokens.push_back(token);
        }

        Ok(())
    }

    fn get_channel(&self, token: mio::Token) -> ChannelPtr {
        if !Self::is_channel_token(token) {
            panic!("bad channel token: {:?}", token);
        }
        let slab_key = Self::token_to_slab_key(token);
        if let Some(c) = self.channels.get(slab_key) {
            c.clone()
        } else {
            panic!("bad slab key {} for channel token: {:?}", slab_key, token);
        }
    }

    fn open_socket(
        my_id: NodeId,
        peer_id: NodeId,
        slab_key: usize,
        poll: &mio::Poll,
    ) -> io::Result<SockPtr> {
        let peer_addr = NODE_TO_ADDR.get(&peer_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("couldn't find peer id: {}", peer_id))
        })?;

        eprintln!("node {}: connecting to peer {} addr: {}", my_id, peer_id, peer_addr);

        let sock_ptr = Arc::new(RefCell::new(mio::net::TcpStream::connect(peer_addr)?));
        poll.register(
            &*sock_ptr.borrow(),
            Self::slab_key_to_token(slab_key),
            mio::Ready::readable() | mio::Ready::writable(),
            mio::PollOpt::edge(),
        )?;

        Ok(sock_ptr)
    }

    fn open_channel(&mut self, peer_id: NodeId) -> io::Result<ChannelPtr> {
        let entry = self.channels.vacant_entry();
        let slab_key = entry.key();
        let sock_ptr = Self::open_socket(self.my_id, peer_id, slab_key, &self.poll)?;
        let channel_ptr =
            Arc::new(RefCell::new(Channel::connecting(self.my_id, peer_id, sock_ptr, slab_key)));
        entry.insert(channel_ptr.clone());
        Ok(channel_ptr)
    }

    fn try_reconnect_channel(
        &mut self,
        channel: &mut Channel,
        output: Arc<PeerOutput>,
    ) -> io::Result<()> {
        assert!(!channel.is_scheduled);
        let sock_ptr =
            Self::open_socket(self.my_id, channel.peer_id, channel.slab_key, &self.poll)?;
        channel.reconnect(sock_ptr);
        channel.output = Some(output);
        self.schedule_channel(channel);
        Ok(())
    }

    fn close_channel(
        &mut self,
        channel: &Channel,
        channel_ptr: &ChannelPtr,
        peer_id2channel: &mut HashMap<NodeId, ChannelPtr>,
    ) {
        assert!(!channel.is_scheduled);
        self.channels.remove(channel.slab_key);
        if let hash_map::Entry::Occupied(entry) = peer_id2channel.entry(channel.peer_id) {
            if Arc::ptr_eq(&channel_ptr, entry.get()) {
                entry.remove();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        std::sync::{Arc, Barrier},
    };

    lazy_static::lazy_static! { static ref SERIALIZE_MUTEX: Mutex<()> = Mutex::new(()); }

    const ID1: NodeId = NodeId(0xdeadbeef);
    const ID2: NodeId = NodeId(0xfeedface);

    #[test]
    fn destroy() {
        let _serialize_guard = SERIALIZE_MUTEX.lock().unwrap();
        let mut node = Some(NetworkNode::bind(ID1));
        node.take();
    }

    #[test]
    fn simultaneous_connect() {
        let _serialize_guard = SERIALIZE_MUTEX.lock().unwrap();
        let barrier = Arc::new(Barrier::new(2));

        let t1 = std::thread::spawn({
            let barrier = barrier.clone();
            move || {
                let (my_id, other_id) = (ID1, ID2);
                let node = NetworkNode::bind(my_id).unwrap();

                barrier.wait();

                node.send(other_id, b"from node1", None).unwrap();
                node.flush();

                let msg = node.recv(None).unwrap();
                assert_eq!(msg.peer_id, other_id);
                assert_eq!(msg.payload, b"from node2");
            }
        });

        let t2 = std::thread::spawn({
            let barrier = barrier.clone();
            move || {
                let (my_id, other_id) = (ID2, ID1);
                let node = NetworkNode::bind(my_id).unwrap();

                barrier.wait();

                node.send(other_id, b"from node2", None).unwrap();
                node.flush();

                let msg = node.recv(None).unwrap();
                assert_eq!(msg.peer_id, other_id);
                assert_eq!(msg.payload, b"from node1");
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn connect_after_send() {
        let _serialize_guard = SERIALIZE_MUTEX.lock().unwrap();
        let node1 = NetworkNode::bind(ID1).unwrap();

        node1.send(ID2, b"hello", None).unwrap();
        std::thread::sleep(Duration::from_millis(1000));

        let node2 = NetworkNode::bind(ID2).unwrap();
        let msg = node2.recv(None).unwrap();
        assert_eq!(msg.peer_id, ID1);
        assert_eq!(msg.payload, b"hello");
    }
}
