#![allow(clippy::derive_partial_eq_without_eq)]
#![allow(clippy::uninlined_format_args)]
use colink::{CoLink, Participant, ProtocolEntry};
use colink_remote_storage_proto::*;
use prost::Message;

mod colink_remote_storage_proto {
    include!(concat!(env!("OUT_DIR"), "/colink_remote_storage.rs"));
}

async fn update_remaining_quota(
    cl: &CoLink,
    requester_uid: &str,
    size: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let remaining_quota_key = format!("_remote_storage:remaining_quota:{}", requester_uid);
    let lock = cl.lock(&remaining_quota_key).await?;
    let mut remaining_quota = match cl.read_entry(&remaining_quota_key).await {
        Ok(data) => i64::from_le_bytes(<[u8; 8]>::try_from(data).unwrap()),
        Err(_) => {
            match cl
                .read_entry("_remote_storage:default_remaining_quota")
                .await
            {
                Ok(data) => i64::from_le_bytes(<[u8; 8]>::try_from(data).unwrap()),
                Err(_) => {
                    cl.create_entry(
                        "_remote_storage:default_remaining_quota",
                        &4194304_i64.to_le_bytes(),
                    )
                    .await?;
                    4194304_i64
                }
            }
        }
    };
    if remaining_quota < size {
        Err("Do not have enough quota.")?
    }
    remaining_quota -= size;
    cl.update_entry(&remaining_quota_key, &remaining_quota.to_le_bytes())
        .await?;
    cl.unlock(lock).await?;
    Ok(())
}

struct Init;
#[colink::async_trait]
impl ProtocolEntry for Init {
    async fn start(
        &self,
        _cl: CoLink,
        _param: Vec<u8>,
        _participants: Vec<Participant>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        Ok(())
    }
}

struct CreateRequester;
#[colink::async_trait]
impl ProtocolEntry for CreateRequester {
    async fn start(
        &self,
        _cl: CoLink,
        _param: Vec<u8>,
        _participants: Vec<Participant>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        Ok(())
    }
}

struct CreateProvider;
#[colink::async_trait]
impl ProtocolEntry for CreateProvider {
    async fn start(
        &self,
        cl: CoLink,
        param: Vec<u8>,
        participants: Vec<Participant>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let params: CreateParams = prost::Message::decode(&*param)?;
        let requester_uid = &participants[0].user_id;
        update_remaining_quota(
            &cl,
            requester_uid,
            (params.remote_key_name.as_bytes().len() + params.payload.len()) as i64,
        )
        .await?;
        cl.create_entry(
            &format!(
                "_remote_storage:{}:{}:{}",
                if params.is_public {
                    "public"
                } else {
                    "private"
                },
                requester_uid,
                params.remote_key_name
            ),
            &params.payload,
        )
        .await?;
        Ok(())
    }
}

struct ReadRequester;
#[colink::async_trait]
impl ProtocolEntry for ReadRequester {
    async fn start(
        &self,
        cl: CoLink,
        _param: Vec<u8>,
        participants: Vec<Participant>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let provider_uid = &participants[1].user_id;
        let key = format!(
            "_remote_storage:private:{}:_variable_transfer:{}:status",
            provider_uid,
            cl.get_task_id()?
        );
        let status = cl.read_or_wait(&key).await?;
        if status[0] == 0 {
            let key = format!(
                "_remote_storage:private:{}:_variable_transfer:{}:output",
                provider_uid,
                cl.get_task_id()?
            );
            let data = cl.read_or_wait(&key).await?;
            cl.create_entry(&format!("tasks:{}:output", cl.get_task_id()?), &data)
                .await?;
            cl.create_entry(&format!("tasks:{}:status", cl.get_task_id()?), &[0])
                .await?;
        } else {
            cl.create_entry(&format!("tasks:{}:status", cl.get_task_id()?), &[1])
                .await?;
        }
        Ok(())
    }
}

struct ReadProvider;
#[colink::async_trait]
impl ProtocolEntry for ReadProvider {
    async fn start(
        &self,
        cl: CoLink,
        param: Vec<u8>,
        participants: Vec<Participant>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let requester_uid = &participants[0].user_id;
        let participants = vec![
            Participant {
                user_id: cl.get_user_id()?,
                role: "requester".to_string(),
            },
            Participant {
                user_id: requester_uid.clone(),
                role: "provider".to_string(),
            },
        ];

        let payload = match async {
            let mut params: ReadParams = prost::Message::decode(&*param)?;
            if params.holder_id == String::default() {
                params.holder_id = requester_uid.clone();
            };
            if !params.is_public && params.holder_id != *requester_uid {
                Err("Permission denied.")?;
            }
            let payload = cl
                .read_entry(&format!(
                    "_remote_storage:{}:{}:{}",
                    if params.is_public {
                        "public"
                    } else {
                        "private"
                    },
                    params.holder_id,
                    params.remote_key_name
                ))
                .await?;
            Ok::<Vec<u8>, Box<dyn std::error::Error + Send + Sync + 'static>>(payload)
        }
        .await
        {
            Ok(payload) => payload,
            Err(e) => {
                self.set_status(&cl, &participants, 1).await?;
                Err(e)?
            }
        };

        self.set_status(&cl, &participants, 0).await?;
        let params = CreateParams {
            remote_key_name: format!("_variable_transfer:{}:output", cl.get_task_id()?),
            payload,
            ..Default::default()
        };
        let mut payload = vec![];
        params.encode(&mut payload).unwrap();
        cl.run_task("remote_storage.create", &payload, &participants, false)
            .await?;
        Ok(())
    }
}

impl ReadProvider {
    async fn set_status(
        &self,
        cl: &CoLink,
        participants: &[Participant],
        status_code: u8,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let params = CreateParams {
            remote_key_name: format!("_variable_transfer:{}:status", cl.get_task_id()?),
            payload: vec![status_code],
            ..Default::default()
        };
        let mut payload = vec![];
        params.encode(&mut payload).unwrap();
        cl.run_task("remote_storage.create", &payload, participants, false)
            .await?;
        Ok(())
    }
}

struct UpdateRequester;
#[colink::async_trait]
impl ProtocolEntry for UpdateRequester {
    async fn start(
        &self,
        _cl: CoLink,
        _param: Vec<u8>,
        _participants: Vec<Participant>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        Ok(())
    }
}

struct UpdateProvider;
#[colink::async_trait]
impl ProtocolEntry for UpdateProvider {
    async fn start(
        &self,
        cl: CoLink,
        param: Vec<u8>,
        participants: Vec<Participant>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let params: UpdateParams = prost::Message::decode(&*param)?;
        let requester_uid = &participants[0].user_id;
        update_remaining_quota(
            &cl,
            requester_uid,
            (params.remote_key_name.as_bytes().len() + params.payload.len()) as i64,
        )
        .await?;
        cl.update_entry(
            &format!(
                "_remote_storage:{}:{}:{}",
                if params.is_public {
                    "public"
                } else {
                    "private"
                },
                requester_uid,
                params.remote_key_name
            ),
            &params.payload,
        )
        .await?;
        Ok(())
    }
}

struct DeleteRequester;
#[colink::async_trait]
impl ProtocolEntry for DeleteRequester {
    async fn start(
        &self,
        _cl: CoLink,
        _param: Vec<u8>,
        _participants: Vec<Participant>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        Ok(())
    }
}

struct DeleteProvider;
#[colink::async_trait]
impl ProtocolEntry for DeleteProvider {
    async fn start(
        &self,
        cl: CoLink,
        param: Vec<u8>,
        participants: Vec<Participant>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let params: UpdateParams = prost::Message::decode(&*param)?;
        let requester_uid = &participants[0].user_id;
        cl.delete_entry(&format!(
            "_remote_storage:{}:{}:{}",
            if params.is_public {
                "public"
            } else {
                "private"
            },
            requester_uid,
            params.remote_key_name
        ))
        .await?;
        Ok(())
    }
}

colink::protocol_start!(
    ("remote_storage:@init", Init),
    ("remote_storage.create:requester", CreateRequester),
    ("remote_storage.create:provider", CreateProvider),
    ("remote_storage.read:requester", ReadRequester),
    ("remote_storage.read:provider", ReadProvider),
    ("remote_storage.update:requester", UpdateRequester),
    ("remote_storage.update:provider", UpdateProvider),
    ("remote_storage.delete:requester", DeleteRequester),
    ("remote_storage.delete:provider", DeleteProvider)
);
