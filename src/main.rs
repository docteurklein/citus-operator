use std::{sync::Arc, time::Duration};

use futures_util::StreamExt;
use thiserror::Error;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use k8s_openapi::{
    apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition,
    apimachinery::pkg::api::resource::Quantity,
};

mod cnpg;

use kube::{
    Client,
    CustomResource,
    CustomResourceExt,
    Api,
    api::{PatchParams, Patch, ListParams},
    runtime::{
        wait::{await_condition, conditions},
        Controller,
        controller::Action
    },
    core::{GroupVersionKind, DynamicObject},
    discovery::ApiResource
};
use serde_json::{Value, json};

#[derive(CustomResource, Debug, Serialize, Deserialize, Default, Clone, JsonSchema)]
#[kube(group = "citus.dev", version = "v1", kind = "Cluster", struct = "CitusCluster", namespaced)]
#[kube(scale = r#"{"specReplicasPath":".spec.replicas", "statusReplicasPath":".status.replicas"}"#)]
#[kube(status = "CitusClusterStatus")]
pub struct CitusClusterSpec {
    nodes: isize,
    node_spec: cnpg::ClusterSpec,
}

// #[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
// pub struct CnpgSpec {
//     instances: isize,
//     storage: Storage,
// }

// #[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
// pub struct Storage {
//     size: String,
// }
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
pub struct CitusClusterStatus {
    is_ok: bool,
    nodes: isize,
}

#[derive(Debug, Error)]
enum Error {
    #[error("MissingObjectKey: {0}")]
    MissingObjectKey(&'static str),
}

struct Data {
    client: Client,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client: Client = Client::try_default().await?;
    let apply = PatchParams::apply("citus_apply").force();

    // install crd
    let crds_api: Api<CustomResourceDefinition> = Api::all(client.clone());
    crds_api.patch("clusters.citus.dev", &apply, &Patch::Apply(CitusCluster::crd())).await?;
    let establish = await_condition(crds_api, "clusters.citus.dev", conditions::is_crd_established());
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), establish).await?;

    let citus_api: Api<CitusCluster> = Api::all(client.clone());

    let gvk = GroupVersionKind::gvk("postgresql.cnpg.io", "v1", "Cluster");
    let ar = ApiResource::from_gvk(&gvk);
    let cnpg_api: Api<DynamicObject> = Api::default_namespaced_with(client.clone(), &ar);   

    Controller::new(citus_api, ListParams::default())
        .owns_with(cnpg_api, ar, ListParams::default())
        .shutdown_on_signal()
        .run(reconcile, error_policy, Arc::new(Data { client }))
        .for_each(|res| async move {
            dbg!(res);
            ()
            // match res {
            //     Ok(o) => dbg!("reconciled {:?}", o),
            //     Err(e) => dbg!("reconcile failed: {}", e),
            // }
        })
        .await
    ;

    
    Ok(())
}

fn error_policy(_object: Arc<CitusCluster>, _error: &kube::Error, _ctx: Arc<Data>) -> Action {
    Action::requeue(Duration::from_secs(1))
}

async fn reconcile(generator: Arc<CitusCluster>, ctx: Arc<Data>) -> Result<Action, kube::Error> {
    // setup cnpg api
    let gvk = GroupVersionKind::gvk("postgresql.cnpg.io", "v1", "Cluster");
    let ar = ApiResource::from_gvk(&gvk);
    let apply = PatchParams::apply("citus_apply").force();
    
    let cnpg_api: Api<DynamicObject> = Api::default_namespaced_with(ctx.client.clone(), &ar);   
    let cnpg = DynamicObject::new("test-cnpg", &ar).data(json!({
        "apiVersion": "postgresql.cnpg.io/v1",
        "kind": "Cluster",
        "metadata": {
            "name": "inner-c",
        },
        "spec": generator.spec.clone().node_spec
    }));
    cnpg_api.patch("inner-c", &apply, &Patch::Apply(&cnpg)).await?;
    Ok(Action::requeue(Duration::from_secs(10)))
}