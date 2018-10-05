extern crate futures;
extern crate tokio_core;
extern crate tokio_io;

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{BufWriter, BufReader, BufRead};
use std::rc::Rc;
use std::env;
use std::net::SocketAddr;
use std::fs::{File, OpenOptions};
use std::io::Write;

use futures::prelude::*;
use tokio_core::net::TcpListener;
use tokio_core::reactor::Core;
use tokio_io::AsyncRead;
use tokio_io::io::{lines, write_all};

/// The in-memory database shared amongst all clients.
///
/// This database will be shared via `Rc`, so to mutate the internal map we're
/// also going to use a `RefCell` for interior mutability.
struct Database {
    map: RefCell<HashMap<String, String>>,
    persist_log: String,
}

/// Possible requests our clients can send us
enum Request {
    Exit {},
    Ping { msg: String },
    Get { key: String },
    Del { key: String },
    Put { key: String, value: String },
}

/// Responses to the `Request` commands above
enum Response {
    Value { key: String, value: String },
    Put { key: String, value: String },
    Del { key: String },
    Error { msg: String },
    Message { msg: String },
}

fn main() {
    // Parse the address we're going to run this server on, create a `Core`, and
    // set up our TCP listener to accept connections.
    let addr = env::args().nth(1).unwrap_or("127.0.0.1:8080".to_string());
    let addr = addr.parse::<SocketAddr>().unwrap();
    let _persist_log_path = env::args().nth(2).unwrap_or("db.log".to_string());
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let listener = TcpListener::bind(&addr, &handle).expect("failed to bind");
    println!("Listening on: {}", addr);

    // Create the shared state of this server that will be shared amongst all
    // clients. We populate the initial database and then create the `Database`
    // structure. Note the usage of `Rc` here which will be used to ensure that
    // each independently spawned client will have a reference to the in-memory
    // database.
    let db = init_database(_persist_log_path);

    let done = listener.incoming().for_each(move |(socket, _addr)| {
        // As with many other small examples, the first thing we'll do is
        // *split* this TCP stream into two separately owned halves. This'll
        // allow us to work with the read and write halves independently.
        let (reader, writer) = socket.split();

        // Since our protocol is line-based we use `tokio_io`'s `lines` utility
        // to convert our stream of bytes, `reader`, into a `Stream` of lines.
        let lines = lines(BufReader::new(reader));

        // Here's where the meat of the processing in this server happens. First
        // we see a clone of the database being created, which is creating a
        // new reference for this connected client to use. Also note the `move`
        // keyword on the closure here which moves ownership of the reference
        // into the closure, which we'll need for spawning the client below.
        //
        // The `map` function here means that we'll run some code for all
        // requests (lines) we receive from the client. The actual handling here
        // is pretty simple, first we parse the request and if it's valid we
        // generate a response based on the values in the database.
        let db = db.clone();
        let responses = lines.map(move |line| { db.process_line(&line, true) });

        // At this point `responses` is a stream of `Response` types which we
        // now want to write back out to the client. To do that we use
        // `Stream::fold` to perform a loop here, serializing each response and
        // then writing it out to the client.
        let writes = responses.fold(writer, |writer, response| {
            let mut answer = response.serialize();
            answer.push('\n');
            print!("{}", answer);
            write_all(writer, answer.into_bytes()).map(|(w, _)| w)
        });

        // Like with other small servers, we'll `spawn` this client to ensure it
        // runs concurrently with all other clients, for now ignoring any errors
        // that we see.
        let msg = writes.then(move |_| Ok(()));
        handle.spawn(msg);
        Ok(())
    });

    core.run(done).unwrap();
}

fn init_database(persist_path: String) -> Rc<Database> {
    let ret = Rc::new(Database {
        map: RefCell::new(HashMap::new()),
        persist_log:  persist_path,
    });
    let reader = BufReader::new(File::open(ret.persist_log.clone()).expect("Unable to open persist logi file"));
    for line in reader.lines() {
        let response = ret.process_line(&line.unwrap(), false);
        println!("{}", response.serialize());
    }
    println!("Persistent storage read");
    return ret;
}

fn append_line( filename: String, line: String ) -> bool {
    let file = OpenOptions::new()
                           .append(true)
                           .create(true)
                           .open(filename)
                           .expect("Unable to open persistent log file");
    let mut writer = BufWriter::new(file);
    return match writeln!(writer, "{}", line) {
        Ok(_) =>  true,
        Err(_) => false,
    };
}

impl Database {
    fn process_line(&self, line: &String, write_log: bool) -> Response { 
            let request = match Request::parse(line) {
                Ok(req) => req,
                Err(e) => return Response::Error { msg: e },
            };

            // TODO: How to make a shared state for persist log file handler?
            match request {
                Request::Get { key } => {
                    match self.get(&key) {
                        Ok(value) => Response::Value { key, value: value.clone() },
                        Err(_error) => Response::Error { msg: format!("no key {}", key) },
                    }
                }
                Request::Del { key } => {
                    if write_log {
                        match append_line( self.persist_log.clone(), format!("DEL {}", key)) {
                            false => return Response::Error{ msg: format!("Error writing to persist log")},   
                            true => {},
                        }
                    }
                    self.remove(&key);
                    Response::Del { key }
                }
                Request::Put { key, value } => {
                    if write_log {
                        match append_line( self.persist_log.clone(), format!("PUT {} {}", key, value)) {
                            false => return Response::Error{ msg: format!("Error writing to persist log")},   
                            true => {},
                        }
                    }
                    self.insert(key.clone(), value.clone());
                    Response::Put { key, value }
                }
                Request::Ping { msg } => {
                    Response::Message { msg: format!("PONG: {}", msg) }
                }
                Request::Exit { } => {
                    Response::Error { msg: format!("EXIT COMMAND NOT IMPLEMENTED YET") }
                }
            }
    }

    fn remove(&self, key: &String) {
        self.map.borrow_mut().remove(key);
    }

    fn insert(&self, key: String, value: String ) {
        self.map.borrow_mut().insert(key, value);
    }
    
    fn get(&self, key: &String) -> Result<String, String> {
        return match self.map.borrow().get(key) {
            Some(value) => Ok(value.to_string()),
            None => Err("Value not found".to_string())
        };
    }
}

impl Request {
    fn parse(input: &str) -> Result<Request, String> {
        let mut parts = input.splitn(3, " ");
        let command = parts.next().unwrap_or("").to_ascii_uppercase();
        match command.as_ref() {
            "GET" => {
                let key = match parts.next() {
                    Some(key) => key,
                    None => return Err(format!("GET must be followed by a key")),
                };
                if parts.next().is_some() {
                    return Err(format!("GET's key must not be followed by anything"))
                }
                Ok(Request::Get { key: key.to_string() })
            }
            "PUT" => {
                let key = match parts.next() {
                    Some(key) => key,
                    None => return Err(format!("PUT must be followed by a key")),
                };
                let value = match parts.next() {
                    Some(value) => value,
                    None => return Err(format!("PUT needs a value")),
                };
                Ok(Request::Put { key: key.to_string(), value: value.to_string() })
            }
            "DEL" => {
                let key = match parts.next() {
                    Some(key) => key,
                    None => return Err(format!("DEL must be followed by a key")),
                };
                if parts.next().is_some() {
                    return Err(format!("DEL`'s key must not be followed by anything"))
                }
                Ok(Request::Del { key: key.to_string() })
            }
            "PING" => {
                Ok(Request::Ping { msg: format!("{}", parts.next().unwrap_or("").to_string()) })
            }
            "EXIT" | "QUIT" => {
                Ok(Request::Exit {})
            }
            _ => Err(format!("unknown command: {}", command)),
        }
    }
}

impl Response {
    fn serialize(&self) -> String {
        match *self {
            Response::Value { ref key, ref value } => {
                format!("GET {} == {}", key, value)
            }
            Response::Del { ref key } => {
                format!("DEL {}", key)
            }
            Response::Put { ref key, ref value } => {
                format!("PUT {} {}", key, value)
            }
            Response::Message { ref msg } => {
                format!("{}", msg)
            }
            Response::Error { ref msg } => {
                format!("error: {}", msg)
            }
        }
    }
}

