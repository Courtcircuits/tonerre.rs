use futures::stream::StreamExt;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rdkafka::consumer::{DefaultConsumerContext, MessageStream};

use rdkafka::{consumer::Consumer as RDKafkaConsumer, consumer::StreamConsumer};

use crate::extract::FromMessage;

pub type SharedHandler =
    Arc<dyn Fn(OwnedMessage) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub type BoxedNext = Arc<
    dyn Fn(OwnedMessage, Vec<SharedHandler>) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync
        + 'static,
>;

pub type SharedPicker = Arc<Picker>;

pub type Picker = Box<
    dyn for<'a> Fn(
            MessageStream<'a, DefaultConsumerContext>,
            BoxedNext,
            Vec<SharedHandler>,
        ) -> Pin<
            Box<dyn Future<Output = Result<(), rdkafka::error::KafkaError>> + Send + 'a>,
        > + Send
        + Sync,
>;

#[macro_export]
macro_rules! picker {
    ($fn:expr) => {
        Box::new(move |stream, next, handlers| Box::pin($fn(stream, next, handlers)))
    };
}

pub fn handler<F, T>(f: F) -> SharedHandler
where
    T: FromMessage + Send + 'static,
    F: Fn(T) + Send + Sync + Copy + 'static,
{
    Arc::new(move |message: OwnedMessage| {
        Box::pin(async move {
            match T::from_request(message).await {
                Ok(data) => f(data),
                Err(_) => {}
            }
        })
    })
}

#[derive(Clone)]
pub struct TopicHandler {
    handlers: Vec<SharedHandler>,
    picker: Option<SharedPicker>,
}

impl TopicHandler {
    pub fn new(handlers: Vec<SharedHandler>, picker: Option<SharedPicker>) -> Self {
        Self { handlers, picker }
    }

    pub async fn listen_topic(
        &self,
        consumer: &StreamConsumer,
        topic: String,
    ) -> Result<(), rdkafka::error::KafkaError> {
        consumer.subscribe(&[topic.as_str()]).unwrap();

        let stream = consumer.stream();

        let handler: BoxedNext = Arc::new(|message: OwnedMessage, handlers: Vec<SharedHandler>| {
            Box::pin(async move {
                for handler in handlers.clone() {
                    // will be working on a zero copy version of this code later.
                    handler(message.clone()).await
                }
            })
        });

        match self.picker.clone() {
            Some(picker) => picker(stream, handler, self.handlers.clone())
                .await
                .unwrap(),
            None => default_handler(stream, handler, self.handlers.clone())
                .await
                .unwrap(),
        }
        Ok(())
    }
}

async fn default_handler(
    mut stream: MessageStream<'_, DefaultConsumerContext>,
    next: BoxedNext,
    handlers: Vec<SharedHandler>,
) -> Result<(), rdkafka::error::KafkaError> {
    while let Some(message) = stream.next().await {
        let message = message?;
        next(BorrowedMessage::detach(&message), handlers.clone()).await;
    }
    Ok(())
}
