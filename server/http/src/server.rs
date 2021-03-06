use futures::sync::mpsc::{channel, Receiver, Sender};
use hyper;
use hyper::Server;

use std::error::Error;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Mutex;

use graph::components::store::StoreEvent;
use graph::data::query::Query;
use graph::data::schema::Schema;
use graph::prelude::{GraphQLServer as GraphQLServerTrait, *};
use graph_graphql::prelude::api_schema;
use service::GraphQLService;

/// Errors that may occur when starting the server.
#[derive(Debug)]
pub enum GraphQLServeError {
    OrphanError,
    BindError(hyper::Error),
}

impl Error for GraphQLServeError {
    fn description(&self) -> &str {
        "Failed to start the server"
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

impl fmt::Display for GraphQLServeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OrphanError: No component set up to handle the queries")
    }
}

impl From<hyper::Error> for GraphQLServeError {
    fn from(err: hyper::Error) -> Self {
        GraphQLServeError::BindError(err)
    }
}

/// A GraphQL server based on Hyper.
pub struct GraphQLServer {
    logger: slog::Logger,
    query_sink: Option<Sender<Query>>,
    schema_event_sink: Sender<SchemaEvent>,
    store_event_sink: Sender<StoreEvent>,
    schema: Arc<Mutex<Option<Schema>>>,
}

impl GraphQLServer {
    /// Creates a new GraphQL server.
    pub fn new(logger: &slog::Logger) -> Self {
        // Create channels for handling incoming schema and store events.
        let (store_sink, store_stream) = channel(100);
        let (schema_event_sink, schema_event_stream) = channel(100);

        // Create a new GraphQL server
        let mut server = GraphQLServer {
            logger: logger.new(o!("component" => "GraphQLServer")),
            query_sink: None,
            schema_event_sink,
            store_event_sink: store_sink,
            schema: Arc::new(Mutex::new(None)),
        };

        // Spawn tasks to handle incoming schema and store events.
        server.handle_schema_events(schema_event_stream);
        server.handle_store_events(store_stream);

        // Return the new server.
        server
    }

    /// Handle incoming schema events.
    fn handle_schema_events(&mut self, stream: Receiver<SchemaEvent>) {
        let logger = self.logger.clone();
        let schema = self.schema.clone();

        tokio::spawn(stream.for_each(move |event| {
            info!(logger, "Received schema event");

            if let SchemaEvent::SchemaAdded(new_schema) = event {
                let mut schema = schema.lock().unwrap();
                let derived_schema = match api_schema(&new_schema.document) {
                    Ok(document) => Schema {
                        id: new_schema.id.clone(),
                        document,
                    },
                    Err(e) => return Ok(error!(logger, "error deriving schema {}", e)),
                };
                *schema = Some(derived_schema);
            } else {
                panic!("schema removal is yet not supported")
            }

            Ok(())
        }));
    }

    // Handle incoming events from the store.
    fn handle_store_events(&mut self, stream: Receiver<StoreEvent>) {
        let logger = self.logger.clone();

        tokio::spawn(stream.for_each(move |event| {
            info!(logger, "Received store event"; "event" => format!("{:?}",  event));
            Ok(())
        }));
    }
}

impl GraphQLServerTrait for GraphQLServer {
    type ServeError = GraphQLServeError;

    fn schema_event_sink(&mut self) -> Sender<SchemaEvent> {
        self.schema_event_sink.clone()
    }

    fn store_event_sink(&mut self) -> Sender<StoreEvent> {
        self.store_event_sink.clone()
    }

    fn query_stream(&mut self) -> Result<Receiver<Query>, StreamError> {
        // If possible, create a new channel for streaming incoming queries
        match self.query_sink {
            Some(_) => Err(StreamError::AlreadyCreated),
            None => {
                let (sink, stream) = channel(100);
                self.query_sink = Some(sink);
                Ok(stream)
            }
        }
    }

    fn serve(
        &mut self,
        port: u16,
    ) -> Result<Box<Future<Item = (), Error = ()> + Send>, Self::ServeError> {
        let logger = self.logger.clone();

        let addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port);

        // Only launch the GraphQL server if there is a component that will handle incoming queries
        let query_sink = self
            .query_sink
            .as_ref()
            .ok_or(GraphQLServeError::OrphanError)?;

        // On every incoming request, launch a new GraphQL service that writes
        // incoming queries to the query sink.
        let query_sink = query_sink.clone();
        let schema = self.schema.clone();
        let new_service = move || {
            let service = GraphQLService::new(schema.clone(), query_sink.clone());
            future::ok::<GraphQLService, hyper::Error>(service)
        };

        // Create a task to run the server and handle HTTP requests
        let task = Server::try_bind(&addr.into())?
            .serve(new_service)
            .map_err(move |e| error!(logger, "Server error"; "error" => format!("{}", e)));

        Ok(Box::new(task))
    }
}

#[test]
fn emits_an_api_schema_after_one_schema_is_added() {
    use graph_graphql::schema::ast;
    use std::ops::Deref;
    use std::time::{Duration, Instant};

    let mut runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(future::lazy(|| {
            let res: Result<_, ()> = Ok({
                // Set up the server
                let logger = Logger::root(slog::Discard, o!());
                let mut server = GraphQLServer::new(&logger);
                let schema_sink = server.schema_event_sink();

                // Create an input schema event
                let input_doc =
                    ::graphql_parser::parse_schema("type User { name: String! }").unwrap();
                let input_schema = Schema {
                    id: "input-schema".to_string(),
                    document: input_doc,
                };
                let input_event = SchemaEvent::SchemaAdded(input_schema.clone());

                // Send the input schema event to the server
                schema_sink.send(input_event).wait().unwrap();

                // Wait for the schema to be received and extract it.
                // Wait for thirty seconds for that to happen, otherwise fail the test.
                let start_time = Instant::now();
                let max_wait = Duration::from_secs(30);
                let output_schema = loop {
                    if let Some(schema) = server.schema.lock().unwrap().deref() {
                        break schema.clone();
                    } else if Instant::now().duration_since(start_time) > max_wait {
                        panic!("Timed out, schema not received")
                    }
                    ::std::thread::yield_now();
                };

                assert_eq!(output_schema.id, input_schema.id);

                // The output schema must include the input schema types
                assert_eq!(
                    ast::get_named_type(&input_schema.document, &"User".to_string()),
                    ast::get_named_type(&output_schema.document, &"User".to_string())
                );

                // The output schema must include a Query type
                ast::get_named_type(&output_schema.document, &"Query".to_string())
                    .expect("Query type missing in output schema");
            });
            res
        }))
        .unwrap();
}
