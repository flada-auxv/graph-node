use futures::sync::mpsc::{channel, Receiver, Sender};
use graphql_parser;
use std::error::Error;
use std::fmt;
use std::sync::Mutex;

use graph::components::schema::SchemaProviderEvent;
use graph::prelude::*;

#[derive(Debug)]
pub struct MockServeError;

impl Error for MockServeError {
    fn description(&self) -> &str {
        "Mock serve error"
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

impl fmt::Display for MockServeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Mock serve error")
    }
}

/// A mock `GraphQLServer`.
pub struct MockGraphQLServer<Q> {
    logger: Logger,
    schema_provider_event_sink: Sender<SchemaProviderEvent>,
    schema: Arc<Mutex<Option<Schema>>>,
    query_runner: Arc<Q>,
}

impl<Q> MockGraphQLServer<Q> {
    /// Creates a new mock `GraphQLServer`.
    pub fn new(logger: &Logger, query_runner: Arc<Q>) -> Self {
        // Create channels for handling incoming events from the schema provider
        let (schema_provider_sink, schema_provider_stream) = channel(100);

        // Create a new mock GraphQL server
        let mut server = MockGraphQLServer {
            logger: logger.new(o!("component" => "MockGraphQLServer")),
            schema_provider_event_sink: schema_provider_sink,
            schema: Arc::new(Mutex::new(None)),
            query_runner,
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
            info!(logger, "Received schema provider event"; "event" => format!("{:?}", event));
            let SchemaProviderEvent::SchemaChanged(new_schema) = event;

            let mut schema = schema.lock().unwrap();
            *schema = new_schema;

            Ok(())
        }));
    }
}

impl<Q> GraphQLServer for MockGraphQLServer<Q>
where
    Q: QueryRunner + 'static,
{
    type ServeError = MockServeError;

    fn schema_provider_event_sink(&mut self) -> Sender<SchemaProviderEvent> {
        self.schema_provider_event_sink.clone()
    }

    fn serve(
        &mut self,
        _port: u16,
    ) -> Result<Box<Future<Item = (), Error = ()> + Send>, Self::ServeError> {
        let schema = self.schema.clone();
        let query_runner = self.query_runner.clone();
        let logger = self.logger.clone();

        // Generate mock query requests
        let requests = (0..5)
            .map(|_| {
                let schema = schema.lock().unwrap();
                Query {
                    schema: schema.clone().unwrap(),
                    document: graphql_parser::parse_query("{ allUsers { name }}").unwrap(),
                    variables: None,
                }
            })
            .collect::<Vec<Query>>();

        println!("Requests: {:?}", requests);

        // Create task to generate mock queries
        Ok(Box::new(stream::iter_ok(requests).for_each(move |query| {
            let logger = logger.clone();
            query_runner.run_query(query).then(move |result| {
                info!(logger, "Query result: {:?}", result);
                Ok(())
            })
        })))
    }
}
