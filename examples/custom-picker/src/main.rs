use std::time::Duration;

use futures::StreamExt;
use rdkafka::{
    ClientConfig, Message,
    consumer::{DefaultConsumerContext, MessageStream},
    message::BorrowedMessage,
};
use tonerre::{
    extract::Raw,
    picker,
    subscriber::Subscriber,
    topic_handler::{BoxedNext, SharedHandler, handler},
};

async fn raw_handler(Raw(message): Raw) {
    let as_string = std::str::from_utf8(message.payload().unwrap()).unwrap();
    println!("Your message : {}", as_string);
}

async fn interval_picker(
    mut stream: MessageStream<'_, DefaultConsumerContext>,
    next: BoxedNext,
    handlers: Vec<SharedHandler>,
) -> Result<(), rdkafka::error::KafkaError> {
    while let Some(message) = stream.next().await {
        let message = message?;

        next(BorrowedMessage::detach(&message), handlers.clone()).await;

        tokio::time::sleep(Duration::from_millis(5000)).await;
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let subscriber = Subscriber::new()
        .subscribe_with_picker(
            "topic1",
            vec![handler(raw_handler)],
            picker!(interval_picker),
        )
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
