use futures::stream::StreamExt;
use rdkafka::message::{BorrowedMessage, OwnedMessage};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rdkafka::consumer::{DefaultConsumerContext, MessageStream};

use rdkafka::{consumer::Consumer as RDKafkaConsumer, consumer::StreamConsumer};


use crate::extract::FromMessage;

pub type SharedHandler<S> =
    Arc<dyn Fn(OwnedMessage, S) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub type BoxedNext = Arc<
    dyn Fn(OwnedMessage) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync
        + 'static,
>;

pub type SharedPicker = Arc<Picker>;

pub type Picker = Box<
    dyn for<'a> Fn(
            MessageStream<'a, DefaultConsumerContext>,
            BoxedNext
        ) -> Pin<
            Box<dyn Future<Output = Result<(), rdkafka::error::KafkaError>> + Send + 'a>,
        > + Send
        + Sync,
>;

#[macro_export]
macro_rules! picker {
    ($fn:expr) => {
        Box::new(move |stream, next| Box::pin($fn(stream, next)))
    };
}

pub fn handler<F, T, S>(f: F) -> SharedHandler<S>
where
    T: FromMessage + Send + 'static,
    S: Send + Sync + Clone + 'static,
    F: Fn(T, S) + Send + Sync + Copy + 'static,
{
    Arc::new(move |message: OwnedMessage, state: S| {
        Box::pin(async move {
            match T::from_request(message).await {
                Ok(data) => f(data, state),
                Err(_) => {
                }
            }
        })
    })
}

#[derive(Clone)]
pub struct TopicHandler<S>
where 
    S: Send + Sync + Clone + 'static
{
    handlers: Vec<SharedHandler<S>>,
    picker: Option<SharedPicker>,
}

impl<S> TopicHandler<S>
where
    S: Send + Sync + Clone + 'static
{
    pub fn new(handlers: Vec<SharedHandler<S>>, picker: Option<SharedPicker>) -> Self {
        Self { handlers, picker }
    }

    pub async fn listen_topic(
        &self,
        consumer: &StreamConsumer,
        topic: String,
        state: S
    ) -> Result<(), rdkafka::error::KafkaError> {
        consumer.subscribe(&[topic.as_str()]).unwrap();

        let stream = consumer.stream();

        let handlers = self.handlers.clone();
        let handler: BoxedNext = Arc::new(move |message: OwnedMessage| {
            let handlers = handlers.clone();
            let state = state.clone();
            Box::pin(async move {
                for handler in handlers.clone() {
                    // will be working on a zero copy version of this code later.
                    handler(message.clone(), state.clone()).await
                }
            })
        });

        match self.picker.clone() {
            Some(picker) => picker(stream, handler)
                .await
                .unwrap(),
            None => default_handler(stream, handler)
                .await
                .unwrap(),
        }
        Ok(())
    }
}

async fn default_handler(
    mut stream: MessageStream<'_, DefaultConsumerContext>,
    next: BoxedNext,
) -> Result<(), rdkafka::error::KafkaError> {
    while let Some(message) = stream.next().await {
        let message = message?;
        next(BorrowedMessage::detach(&message)).await;
    }
    Ok(())
}
