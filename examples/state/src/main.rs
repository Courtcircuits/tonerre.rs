use std::sync::Arc;

use rdkafka::{ClientConfig, Message};
use tokio::sync::Mutex;
use tonerre::{extract::Raw, subscriber::Subscriber, topic_handler::handler};

struct State {
    counter: i32
}

async fn raw_handler(Raw(message): Raw, state: Arc<Mutex<State>>) {
    let as_string = std::str::from_utf8(message.payload().unwrap()).unwrap();
    let mut counter = state.lock().await.unwrap();

    println!("Your message : {}", as_string);
}

#[tokio::main]
async fn main() {
    let state = Arc::new(Mutex::new(State{
        counter: 0
    }));

    let subscriber = Subscriber::new(state)
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
