An event loop and a simple messaging layer over TCP sockets. This is a toy
project created with educational purposes. It is not intended for general use.

# Example

Bind to an address, send and receive a message:

```rust
let addr_table = AddressTable([
    (NodeId(0xdeadbeef), "127.0.0.1:2000"),
    (NodeId(0xfeedface), "127.0.0.1:2001"),
].iter().map(|(id, addr)| (*id, addr.parse().unwrap())).collect());

let node = NetworkNode::bind(NodeId(0xdeadbeef), addr_table).unwrap();

node.send(NodeId(0xfeedface), b"hello", None).unwrap();

let msg = node.recv(None).unwrap();
println!("recv msg: {} from {}", String::from_utf8(msg.payload).unwrap(), msg.peer_id);
```