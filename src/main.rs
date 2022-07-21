use colink_sdk_a::{CoLink, Participant};
use colink_sdk_p::ProtocolEntry;
use prost::Message;
use remote_storage_proto::*;

mod remote_storage_proto {
    include!(concat!(env!("OUT_DIR"), "/remote_storage.rs"));
}

async fn update_remaining_quota(
    cl: &CoLink,
    requester_uid: &str,
    size: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let remaining_quota_key = format!("remote_storage:remaining_quota:{}", requester_uid);
    let lock = cl.lock(&remaining_quota_key).await?;
    let mut remaining_quota = match cl.read_entry(&remaining_quota_key).await {
        Ok(data) => i64::from_le_bytes(<[u8; 8]>::try_from(data).unwrap()),
        Err(_) => {
            match cl
                .read_entry("remote_storage:default_remaining_quota")
                .await
            {
                Ok(data) => i64::from_le_bytes(<[u8; 8]>::try_from(data).unwrap()),
                Err(_) => {
                    cl.create_entry(
                        "remote_storage:default_remaining_quota",
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

struct CreateRequester;
#[colink_sdk_p::async_trait]
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
#[colink_sdk_p::async_trait]
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
                "remote_storage:{}:{}:{}",
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
#[colink_sdk_p::async_trait]
impl ProtocolEntry for ReadRequester {
    async fn start(
        &self,
        cl: CoLink,
        _param: Vec<u8>,
        participants: Vec<Participant>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let provider_uid = &participants[1].user_id;
        let key = format!(
            "remote_storage:private:{}:variable_transfer:{}:output",
            provider_uid,
            cl.get_task_id()?
        );
        cl.read_or_wait(&key).await?;
        cl.create_entry(
            &format!("tasks:{}:output", cl.get_task_id()?),
            key.as_bytes(),
        )
        .await?;
        Ok(())
    }
}

struct ReadProvider;
#[colink_sdk_p::async_trait]
impl ProtocolEntry for ReadProvider {
    async fn start(
        &self,
        cl: CoLink,
        param: Vec<u8>,
        participants: Vec<Participant>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let mut params: ReadParams = prost::Message::decode(&*param)?;
        let requester_uid = &participants[0].user_id;
        if params.holder_id == String::default() {
            params.holder_id = requester_uid.clone();
        };
        if !params.is_public && params.holder_id != *requester_uid {
            Err("Permission denied.")?;
        }
        let payload = cl
            .read_entry(&format!(
                "remote_storage:{}:{}:{}",
                if params.is_public {
                    "public"
                } else {
                    "private"
                },
                requester_uid,
                params.remote_key_name
            ))
            .await?;

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
        let params = CreateParams {
            remote_key_name: format!("variable_transfer:{}:output", cl.get_task_id()?),
            payload,
            ..Default::default()
        };
        let mut payload = vec![];
        params.encode(&mut payload).unwrap();
        cl.run_task("remote_storage.create", &payload, &participants, true)
            .await?;
        Ok(())
    }
}

struct UpdateRequester;
#[colink_sdk_p::async_trait]
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
#[colink_sdk_p::async_trait]
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
                "remote_storage:{}:{}:{}",
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
#[colink_sdk_p::async_trait]
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
#[colink_sdk_p::async_trait]
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
            "remote_storage:{}:{}:{}",
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

colink_sdk_p::protocol_start!(
    ("remote_storage.create:requester", CreateRequester),
    ("remote_storage.create:provider", CreateProvider),
    ("remote_storage.read:requester", ReadRequester),
    ("remote_storage.read:provider", ReadProvider),
    ("remote_storage.update:requester", UpdateRequester),
    ("remote_storage.update:provider", UpdateProvider),
    ("remote_storage.delete:requester", DeleteRequester),
    ("remote_storage.delete:provider", DeleteProvider)
);
