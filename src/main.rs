use colink_sdk_a::{CoLink, Participant};
use colink_sdk_p::ProtocolEntry;
use prost::Message;
use remote_storage_proto::*;

mod remote_storage_proto {
    include!(concat!(env!("OUT_DIR"), "/remote_storage.rs"));
}

struct CreateRequester;
#[colink_sdk_p::async_trait]
impl ProtocolEntry for CreateRequester {
    async fn start(
        &self,
        _cl: CoLink,
        _param: Vec<u8>,
        _participants: Vec<Participant>,
    ) -> Result<(), Box<dyn std::error::Error>> {
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
    ) -> Result<(), Box<dyn std::error::Error>> {
        let params: CreateParams = prost::Message::decode(&*param)?;
        let requester_uid = &participants[0].user_id;
        let remaining_quota = match cl
            .read_entry(&format!("remote_storage:remaining_quota:{}", requester_uid))
            .await
        {
            Ok(data) => i32::from_le_bytes(<[u8; 4]>::try_from(data).unwrap()),
            Err(_) => 64_i32,
        };
        let remaining_quota = remaining_quota - 1;
        cl.update_entry(
            &format!("remote_storage:remaining_quota:{}", requester_uid),
            &remaining_quota.to_le_bytes(),
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
    ) -> Result<(), Box<dyn std::error::Error>> {
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
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut params: ReadParams = prost::Message::decode(&*param)?;
        let requester_uid = &participants[0].user_id;
        // let remaining_quota = match cl
        //     .read_entry(&format!("remote_storage:remaining_quota:{}", requester_uid))
        //     .await
        // {
        //     Ok(data) => i32::from_le_bytes(<[u8; 4]>::try_from(data).unwrap()),
        //     Err(_) => 64_i32,
        // };
        // let remaining_quota = remaining_quota - 1;
        // cl.update_entry(
        //     &format!("remote_storage:remaining_quota:{}", requester_uid),
        //     &remaining_quota.to_le_bytes(),
        // )
        // .await?;
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
                ptype: "requester".to_string(),
            },
            Participant {
                user_id: requester_uid.clone(),
                ptype: "provider".to_string(),
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

colink_sdk_p::protocol_start!(
    ("remote_storage.create:requester", CreateRequester),
    ("remote_storage.create:provider", CreateProvider),
    ("remote_storage.read:requester", ReadRequester),
    ("remote_storage.read:provider", ReadProvider)
);
