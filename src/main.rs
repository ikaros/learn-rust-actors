use actix::prelude::*;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate eventsourcing;
extern crate serde_json;
#[macro_use]
extern crate eventsourcing_derive;

use eventsourcing::{eventstore::MemoryEventStore, prelude::*, Result as EsResult};

struct Organization {
    pubsub: nats::Client,
}

#[derive(Clone, Debug)]
struct OrgData {
    taken_usernames: HashSet<String>,
    generation: u64,
}

impl AggregateState for OrgData {
    fn generation(&self) -> u64 {
        self.generation
    }
}

impl Organization {
    fn new() -> anyhow::Result<Self> {
        let pubsub = nats::Client::new("nats://localhost")?;
        Ok(Organization { pubsub })
    }
}

impl Actor for Organization {
    type Context = Context<Self>;
}

impl Handler<OrgCommand> for Organization {
    type Result = CommandResult;

    fn handle(&mut self, cmd: OrgCommand, _ctx: &mut Context<Self>) -> Self::Result {
        self.handle_command(self)
    }
}

impl Aggregate for Organization {
    type Event = OrgEvent;
    type Command = OrgCommand;
    type State = OrgData;

    fn apply_event(state: &Self::State, evt: &Self::Event) -> EsResult<Self::State> {
        let s = state.clone();
        match *evt {
            OrgEvent::UserRegistered { username, email } => {
                s.taken_usernames.insert(username);
            }
            OrgEvent::UserDeleted { username } => {
                s.taken_usernames.remove(&username);
            }
        };
        s.generation += 1;
        Ok(s)
    }

    fn handle_command(_state: &Self::State, cmd: &Self::Command) -> EsResult<Vec<Self::Event>> {
        let evt = match cmd {
            OrgCommand::RegisterUser { username, email } => {
                if _state.taken_usernames.contains(username) {
                    return Err(eventsourcing::Error {
                        kind: eventsourcing::Kind::CommandFailure("username taken".to_string()),
                    });
                }
                OrgEvent::UserRegistered {
                    username: username.to_string(),
                    email: email.to_string(),
                }
            }
            OrgCommand::DeleteUser { username } => OrgEvent::UserDeleted {
                username: username.to_string(),
            },
        };

        // if validation passes...
        Ok(vec![evt])
    }
}

type CommandResult = anyhow::Result<()>;

#[derive(Message, Debug)]
#[rtype(result = "CommandResult")]
enum OrgCommand {
    RegisterUser { username: String, email: String },
    DeleteUser { username: String },
}

const DOMAIN_VERSION: &str = "1.0";

#[derive(Message, Serialize, Deserialize, Debug, Clone, Event)]
#[rtype(result = "usize")]
#[event_type_version(DOMAIN_VERSION)]
#[event_source("events://kanello.de/organization")]
enum OrgEvent {
    UserRegistered { username: String, email: String },
    UserDeleted { username: String },
}

#[actix_rt::main]
async fn main() -> anyhow::Result<()> {
    let addr = Organization::new()?.start();

    let res = addr
        .send(OrgCommand::RegisterUser {
            username: "Peter".to_string(),
            email: "peter@foo.bar".to_string(),
        })
        .await;

    println!("RESULT; {:?}", res.unwrap());

    System::current().stop();
    Ok(())
}
