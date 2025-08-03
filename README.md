<div align="center">
    <h1>Tonerre.rs</h1>
    <h2>Ergonomic and modular Kafka framework made for Rust built with Rust-Rdkafka and Tokio ⚡</h2>
</div>
</br>

<b>⚠️ This project is still expiremental and should not used for data-intesive productions (yet).</b>

If you're like me, you must have often struggled building clean kafka clients in Rust and you really wish there was a library with a DX similar to [Axum](https://github.com/tokio-rs/axum) that provides a modula way to build Kafka consumers on top of [Rust-Rdkafka](https://github.com/fede1024/rust-rdkafka). Something that handles without too much boilerplate message deserialization, handler matching based on the message type, message picking logic... That's what Tonerre aims to help you with.

## High level features 

- Message handling with deserialization based on extractors similarly to what [Axum](https://docs.rs/axum/latest/axum/extract/index.html).
- Helpers for message deserialization.
- HTTP-like (to not say express like) API to build your consumers with minimum boilerplate.
- Extensible extractors and message pickers if you need to read your topics wisely.

## Feature to be implemented

- [ ] Zero copy message handling and all over the code.
- [ ] Adding support for Protobuf and Avro deserialization.
- [ ] Build a helper for building your producer easily.
- [ ] Adding support for various asynchronous runtimes

Please send me an email at radulescutristan@proton.me with the topic "[tonerre] Hi mihai ...", or just open an issue if you need any other feature non-listed above.

## Usage example

```rust
use rdkafka::{ClientConfig, Message};
use tonerre::{extract::Raw, subscriber::Subscriber, topic_handler::handler};

fn raw_handler(Raw(message): Raw) {
    let as_string = std::str::from_utf8(message.payload().unwrap()).unwrap();
    println!("Your message : {}", as_string);
}

#[tokio::main]
async fn main() {
    let subscriber = Subscriber::new()
        .subscribe("topic1", vec![handler(raw_handler)])
        .complete();

    let mut config = ClientConfig::new();

    config
        .set("group.id", "test-213")
        .set("bootstrap.servers", "localhost:19092")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true");

    subscriber.listen(config).await.unwrap();
}
```

You can find this example at `./examples/basic/`.
