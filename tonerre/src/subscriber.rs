use futures::future::try_join_all;
use rdkafka::ClientConfig;
use rdkafka::consumer::StreamConsumer;
use std::{collections::HashMap, sync::Arc};
use tokio::task::JoinHandle;

use crate::topic_handler::{Picker, SharedHandler, TopicHandler};

#[derive(Clone)]
pub struct InnerSubscriber<S>
where
    S: Send + Sync + Clone + 'static
{
    handlers: Arc<HashMap<String, TopicHandler<S>>>,
    state: S
}

pub struct Subscriber<S> 
where
    S: Send + Sync + Clone + 'static
{
    handlers: HashMap<String, TopicHandler<S>>,
    state: S
}

impl<S> Subscriber<S>
where 
    S: Send + Sync + Clone + 'static
{
    pub fn new(state: S) -> Self {
        Self {
            handlers: HashMap::new(),
            state
        }
    }

    pub fn subscribe(&mut self, topic: &str, handlers: Vec<SharedHandler<S>>) -> &mut Self {
        self.handlers
            .insert(topic.to_string(), TopicHandler::new(handlers, None));
        self
    }

    pub fn subscribe_with_picker(&mut self, topic: &str, handlers: Vec<SharedHandler<S>>, picker: Picker) -> &mut Self {
        self.handlers.insert(topic.to_string(), TopicHandler::new(handlers, Some(Arc::new(picker))));
        self
    }


    pub fn complete(&mut self) -> InnerSubscriber<S> {
        InnerSubscriber::new(self.handlers.clone(), self.state.clone())
    }
}


pub struct DefaultState {

}

impl<S> InnerSubscriber<S> 
where
    S: Send + Sync + Clone + 'static
{
    pub fn new(handlers: HashMap<String, TopicHandler<S>>, state: S) -> Self {
        Self {
            handlers: Arc::new(handlers),
            state,
        }
    }

    pub async fn listen(&self, config: ClientConfig) -> Result<(), rdkafka::error::KafkaError> {
        let consumer: Arc<StreamConsumer> = Arc::new(config.create()?);
        let topics_names: Vec<String> = self.handlers.keys().map(|x| x.to_string()).collect();

        let mut handles = Vec::new();
        for topic in topics_names.iter() {
            let topic = topic.clone();
            let consumer = consumer.clone();
            let handlers = self.handlers.clone();
            let state = self.state.clone();
            let handle: JoinHandle<Result<(), rdkafka::error::KafkaError>> =
                tokio::spawn(async move {
                    let topic_handler = handlers.get(&topic).unwrap();
                    topic_handler.listen_topic(&consumer, topic, state).await
                });
            handles.push(handle);
        }

        match try_join_all(handles).await {
            Ok(_) => {}
            Err(_) => {
            }
        }
        Ok(())
    }
}
