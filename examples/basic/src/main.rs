use rdkafka::{ClientConfig, Message};
use tonerre::{extract::Raw, subscriber::Subscriber, topic_handler::handler};

async fn raw_handler(Raw(message): Raw) {
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
