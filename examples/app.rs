use colink_sdk_a::{decode_jwt_without_validation, CoLink, Participant, SubscriptionMessage};
use prost::Message;
use remote_storage_proto::*;
use std::env;

mod remote_storage_proto {
    include!(concat!(env!("OUT_DIR"), "/remote_storage.rs"));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
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
            role: "requester".to_string(),
        },
        Participant {
            user_id: user_id_b.to_string(),
            role: "provider".to_string(),
        },
    ];
    let cl = CoLink::new(addr, jwt_a);
    // create
    let params = CreateParams {
        remote_key_name: "remote_storage_demo".to_string(),
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
            "_remote_storage:private:{}:remote_storage_demo",
            user_id_a
        ))
        .await?;
    println!("{}", String::from_utf8_lossy(&data));

    // read
    let params = ReadParams {
        remote_key_name: "remote_storage_demo".to_string(),
        ..Default::default()
    };
    let mut payload = vec![];
    params.encode(&mut payload).unwrap();
    let task_id = cl
        .run_task("remote_storage.read", &payload, &participants, true)
        .await?;
    let data = cl
        .read_or_wait(&format!("tasks:{}:output", task_id))
        .await?;
    let path = String::from_utf8_lossy(&data);
    let data = cl.read_or_wait(&path).await?;
    println!("{}", String::from_utf8_lossy(&data));

    // update
    let queue_name = clb
        .subscribe(
            &format!("_remote_storage:private:{}:remote_storage_demo", user_id_a),
            None,
        )
        .await?;

    let params = UpdateParams {
        remote_key_name: "remote_storage_demo".to_string(),
        payload: format!("update {}", msg).as_bytes().to_vec(),
        ..Default::default()
    };
    let mut payload = vec![];
    params.encode(&mut payload).unwrap();
    cl.run_task("remote_storage.update", &payload, &participants, true)
        .await?;

    let mut subscriber = clb.new_subscriber(&queue_name).await?;
    let data = subscriber.get_next().await?;
    let message: SubscriptionMessage = prost::Message::decode(&*data).unwrap();
    if message.change_type != "delete" {
        println!("{}", String::from_utf8_lossy(&*message.payload));
    } else {
        Err("Receive delete change_type.")?
    }

    // delete
    let params = DeleteParams {
        remote_key_name: "remote_storage_demo".to_string(),
        ..Default::default()
    };
    let mut payload = vec![];
    params.encode(&mut payload).unwrap();
    cl.run_task("remote_storage.delete", &payload, &participants, true)
        .await?;

    let data = subscriber.get_next().await?;
    clb.unsubscribe(&queue_name).await?;
    let message: SubscriptionMessage = prost::Message::decode(&*data).unwrap();
    if message.change_type == "delete" {
        println!("Deleted");
    } else {
        Err("Receive non-delete change_type.")?
    }

    Ok(())
}
