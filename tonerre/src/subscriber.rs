use futures::future::try_join_all;
use rdkafka::ClientConfig;
use rdkafka::consumer::StreamConsumer;
use std::{collections::HashMap, sync::Arc};
use tokio::task::JoinHandle;

use crate::topic_handler::{Picker, SharedHandler, TopicHandler};

#[derive(Clone)]
pub struct InnerSubscriber {
    handlers: Arc<HashMap<String, TopicHandler>>,
}

impl Default for Subscriber {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Subscriber {
    handlers: HashMap<String, TopicHandler>,
}

impl Subscriber {
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    pub fn subscribe(&mut self, topic: &str, handlers: Vec<SharedHandler>) -> &mut Self {
        self.handlers
            .insert(topic.to_string(), TopicHandler::new(handlers, None));
        self
    }

    pub fn subscribe_with_picker(
        &mut self,
        topic: &str,
        handlers: Vec<SharedHandler>,
        picker: Picker,
    ) -> &mut Self {
        self.handlers.insert(
            topic.to_string(),
            TopicHandler::new(handlers, Some(Arc::new(picker))),
        );
        self
    }

    pub fn complete(&mut self) -> InnerSubscriber {
        InnerSubscriber::new(self.handlers.clone())
    }
}

impl InnerSubscriber {
    pub fn new(handlers: HashMap<String, TopicHandler>) -> Self {
        Self {
            handlers: Arc::new(handlers),
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
            let handle: JoinHandle<Result<(), rdkafka::error::KafkaError>> =
                tokio::spawn(async move {
                    let topic_handler = handlers.get(&topic).unwrap();
                    topic_handler.listen_topic(&consumer, topic).await
                });
            handles.push(handle);
        }

        let _ = (try_join_all(handles).await).is_ok();
        Ok(())
    }
}
