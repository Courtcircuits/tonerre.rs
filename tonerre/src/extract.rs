use rdkafka::message::{Message, OwnedMessage};

#[cfg(feature = "json_extractor")]
use serde::de::DeserializeOwned;

pub trait FromMessage: Sized + Clone + Send + Sync {
    type Rejection: Send;
    fn from_request(
        message: OwnedMessage,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send;
}

#[derive(Debug, Clone)]
pub struct Raw(pub OwnedMessage);

pub enum RawError {}

impl FromMessage for Raw {
    type Rejection = RawError;
    async fn from_request(message: OwnedMessage) -> Result<Self, Self::Rejection> {
        Ok(Self(message))
    }
}

#[cfg(feature = "json_extractor")]
#[derive(Debug, Clone, Copy, Default)]
pub struct Json<T>(pub T);

#[cfg(feature = "json_extractor")]
pub enum JsonError {
    ParseError,
}

#[cfg(feature = "json_extractor")]
impl<T> FromMessage for Json<T>
where
    T: DeserializeOwned + Clone + Send + Sync,
{
    type Rejection = JsonError;
    async fn from_request(message: OwnedMessage) -> Result<Self, Self::Rejection> {
        let bytes = message.payload().unwrap();

        match Self::from_bytes(bytes) {
            Ok(data) => Ok(data),
            Err(_) => Err(JsonError::ParseError),
        }
    }
}

#[cfg(feature = "json_extractor")]
impl<T> Json<T>
where
    T: DeserializeOwned,
{
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let deserializer = &mut serde_json::Deserializer::from_slice(bytes);
        let res = serde_path_to_error::deserialize(deserializer)?;
        Ok(Self(res))
    }
}

#[cfg(test)]
mod extract_test_json {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Serialize, Deserialize)]
    struct User {
        pub name: String,
        pub email: String,
    }

    #[cfg(feature = "json_extractor")]
    #[test]
    fn test_basic_extract_json() {
        fn test_extractor(Json(user): Json<User>) -> User {
            user
        }

        let user = test_extractor(Json(User {
            name: "Tristan".to_string(),
            email: "test@test.com".to_string(),
        }));

        assert_eq!(user.name, "Tristan".to_string());
        assert_eq!(user.email, "test@test.com".to_string());
    }
}
