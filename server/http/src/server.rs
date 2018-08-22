use futures::sync::mpsc::{channel, Receiver, Sender};
use hyper;
use hyper::Server;

use std::error::Error;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Mutex;

use graph::components::schema::SchemaProviderEvent;
use graph::data::schema::Schema;
use graph::prelude::{GraphQLServer as GraphQLServerTrait, *};

use service::GraphQLService;

/// Errors that may occur when starting the server.
#[derive(Debug)]
pub enum GraphQLServeError {
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
        match self {
            GraphQLServeError::BindError(e) => write!(f, "Failed to bind GraphQL server: {}", e),
        }
    }
}

impl From<hyper::Error> for GraphQLServeError {
    fn from(err: hyper::Error) -> Self {
        GraphQLServeError::BindError(err)
    }
}

/// A GraphQL server based on Hyper.
pub struct GraphQLServer<Q> {
    logger: slog::Logger,
    schema_provider_event_sink: Sender<SchemaProviderEvent>,
    schema: Arc<Mutex<Option<Schema>>>,
    query_runner: Arc<Q>,
}

impl<Q> GraphQLServer<Q> {
    /// Creates a new GraphQL server.
    pub fn new(logger: &slog::Logger, query_runner: Arc<Q>) -> Self {
        // Create channels for handling incoming events from the schema provider
        let (schema_provider_sink, schema_provider_stream) = channel(100);

        // Create a new GraphQL server
        let mut server = GraphQLServer {
            logger: logger.new(o!("component" => "GraphQLServer")),
            schema_provider_event_sink: schema_provider_sink,
            schema: Arc::new(Mutex::new(None)),
            query_runner: query_runner,
        };

        // Spawn tasks to handle incoming events from the schema provider
        server.handle_schema_provider_events(schema_provider_stream);

        // Return the new server
        server
    }

    /// Handle incoming events from the schema provider
    fn handle_schema_provider_events(&mut self, stream: Receiver<SchemaProviderEvent>) {
        let logger = self.logger.clone();
        let schema = self.schema.clone();

        tokio::spawn(stream.for_each(move |event| {
            info!(logger, "Received schema provider event");

            let SchemaProviderEvent::SchemaChanged(new_schema) = event;
            let mut schema = schema.lock().unwrap();
            *schema = new_schema;

            Ok(())
        }));
    }
}

impl<Q> GraphQLServerTrait for GraphQLServer<Q>
where
    Q: GraphQLRunner + Sized + 'static,
{
    type ServeError = GraphQLServeError;

    fn schema_provider_event_sink(&mut self) -> Sender<SchemaProviderEvent> {
        self.schema_provider_event_sink.clone()
    }

    fn serve(
        &mut self,
        port: u16,
    ) -> Result<Box<Future<Item = (), Error = ()> + Send>, Self::ServeError> {
        let logger = self.logger.clone();

        let addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port);

        // On every incoming request, launch a new GraphQL service that writes
        // incoming queries to the query sink.
        let query_runner = self.query_runner.clone();
        let schema = self.schema.clone();
        let new_service = move || {
            let service = GraphQLService::new(schema.clone(), query_runner.clone());
            future::ok::<GraphQLService<Q>, hyper::Error>(service)
        };

        // Create a task to run the server and handle HTTP requests
        let task = Server::try_bind(&addr.into())?
            .serve(new_service)
            .map_err(move |e| error!(logger, "Server error"; "error" => format!("{}", e)));

        Ok(Box::new(task))
    }
}
