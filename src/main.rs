use colink_sdk_a::{CoLink, Participant};
use colink_sdk_p::ProtocolEntry;
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

colink_sdk_p::protocol_start!(
    ("remote_storage.create:requester", CreateRequester),
    ("remote_storage.create:provider", CreateProvider)
);
