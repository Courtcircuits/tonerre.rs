use rdkafka::ClientConfig;
use serde::Deserialize;
use tonerre::{extract::Json, subscriber::Subscriber, topic_handler::handler};

#[derive(Debug, Deserialize, Clone)]
struct User {
    name: String,
}

#[derive(Debug, Deserialize, Clone)]
struct Bike {
    wheels: String,
    color: String,
}

fn handler_1(Json(message): Json<User>) {
    println!("handler_1: {}", message.name);
}

fn handler_2(Json(message): Json<Bike>) {
    println!("handler_2: {}", message.wheels);
    println!("handler_2: {}", message.color);
}

#[tokio::main]
async fn main() {
    let subscriber = Subscriber::new()
        .subscribe("topic1", vec![handler(handler_1), handler(handler_2)])
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
