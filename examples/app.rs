use colink_sdk_a::{decode_jwt_without_validation, CoLink, Participant};
use prost::Message;
use remote_storage_proto::*;
use std::env;

mod remote_storage_proto {
    include!(concat!(env!("OUT_DIR"), "/remote_storage.rs"));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let addr = &args[0];
    let jwt_a = &args[1];
    let jwt_b = &args[2];
    let msg = if args.len() > 3 { &args[3] } else { "hello" };
    let user_id_a = decode_jwt_without_validation(jwt_a).unwrap().user_id;
    let user_id_b = decode_jwt_without_validation(jwt_b).unwrap().user_id;

    let participants = vec![
        Participant {
            user_id: user_id_a.to_string(),
            ptype: "requester".to_string(),
        },
        Participant {
            user_id: user_id_b.to_string(),
            ptype: "provider".to_string(),
        },
    ];
    let cl = CoLink::new(addr, jwt_a);
    let params = CreateParams {
        remote_key_name: "remote_storage_test".to_string(),
        payload: msg.as_bytes().to_vec(),
        ..Default::default()
    };
    let mut payload = vec![];
    params.encode(&mut payload).unwrap();
    cl.run_task("remote_storage.create", &payload, &participants, true)
        .await?;

    let clb = CoLink::new(addr, jwt_b);
    let data = clb
        .read_or_wait(&format!(
            "remote_storage:private:{}:remote_storage_test",
            user_id_a
        ))
        .await?;
    println!("{}", String::from_utf8_lossy(&data));
    Ok(())
}
