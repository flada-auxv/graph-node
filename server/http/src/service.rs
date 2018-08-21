use hyper::service::Service;
use hyper::{Body, Method, Request, Response, StatusCode};
use std::sync::Mutex;

use graph::components::server::GraphQLServerError;
use graph::prelude::*;

use request::GraphQLRequest;
use response::GraphQLResponse;

/// An asynchronous response to a GraphQL request.
pub type GraphQLServiceResponse =
    Box<Future<Item = Response<Body>, Error = GraphQLServerError> + Send>;

/// A Hyper Service that serves GraphQL over a POST / endpoint.
#[derive(Debug)]
pub struct GraphQLService<Q> {
    schema: Arc<Mutex<Option<Schema>>>,
    query_runner: Arc<Q>,
}

impl<Q> GraphQLService<Q>
where
    Q: QueryRunner + 'static,
{
    /// Creates a new GraphQL service.
    pub fn new(schema: Arc<Mutex<Option<Schema>>>, query_runner: Arc<Q>) -> Self {
        GraphQLService {
            schema,
            query_runner,
        }
    }

    /// Serves a GraphiQL index.html.
    fn serve_file(&self, contents: &'static str) -> GraphQLServiceResponse {
        Box::new(future::ok(
            Response::builder()
                .status(200)
                .body(Body::from(contents))
                .unwrap(),
        ))
    }

    /// Handles GraphQL queries received via POST /.
    fn handle_graphql_query(&self, request: Request<Body>) -> GraphQLServiceResponse {
        let query_runner = self.query_runner.clone();
        let schema = self.schema.clone();

        Box::new(
            request
                .into_body()
                .concat2()
                .map_err(|_| GraphQLServerError::from("Failed to read request body"))
                .and_then(move |body| {
                    let schema = schema.lock().unwrap();
                    GraphQLRequest::new(body, schema.clone())
                })
                .and_then(move |query| {
                    // Run the query using the query runner
                    query_runner
                        .run_query(query)
                        .map_err(|e| GraphQLServerError::from(e))
                })
                .then(|result| GraphQLResponse::new(result)),
        )
    }

    // Handles OPTIONS requests
    fn handle_graphql_options(&self, _request: Request<Body>) -> GraphQLServiceResponse {
        Box::new(future::ok(
            Response::builder()
                .status(200)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Headers", "Content-Type")
                .body(Body::from(""))
                .unwrap(),
        ))
    }

    /// Handles 404s.
    fn handle_not_found(&self, _req: Request<Body>) -> GraphQLServiceResponse {
        Box::new(future::ok(
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from("Not found"))
                .unwrap(),
        ))
    }
}

impl<Q> Service for GraphQLService<Q>
where
    Q: QueryRunner + 'static,
{
    type ReqBody = Body;
    type ResBody = Body;
    type Error = GraphQLServerError;
    type Future = GraphQLServiceResponse;

    fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
        match (req.method(), req.uri().path()) {
            // GraphiQL
            (&Method::GET, "/") => self.serve_file(include_str!("../assets/index.html")),
            (&Method::GET, "/graphiql.css") => {
                self.serve_file(include_str!("../assets/graphiql.css"))
            }
            (&Method::GET, "/graphiql.min.js") => {
                self.serve_file(include_str!("../assets/graphiql.min.js"))
            }

            // POST / receives GraphQL queries
            (&Method::POST, "/graphql") => self.handle_graphql_query(req),

            // OPTIONS / allows to check for GraphQL HTTP features
            (&Method::OPTIONS, "/graphql") => self.handle_graphql_options(req),

            // Everything else results in a 404
            _ => self.handle_not_found(req),
        }
    }
}

#[cfg(test)]
mod tests {
    use graphql_parser;
    use graphql_parser::query as q;
    use http::status::StatusCode;
    use hyper::service::Service;
    use hyper::{Body, Method, Request};
    use std::collections::BTreeMap;
    use std::iter::FromIterator;
    use std::sync::Mutex;

    use graph::prelude::*;

    use super::GraphQLService;
    use test_utils;

    /// A simple stupid query runner for testing.
    #[derive(Default)]
    pub struct TestQueryRunner;

    impl QueryRunner for TestQueryRunner {
        fn run_query(
            &self,
            _query: Query,
        ) -> Box<Future<Item = QueryResult, Error = QueryError> + Send> {
            Box::new(future::ok(QueryResult::new(Some(q::Value::Object(
                BTreeMap::from_iter(
                    vec![(
                        String::from("name"),
                        q::Value::String(String::from("Jordi")),
                    )].into_iter(),
                ),
            )))))
        }
    }

    #[test]
    fn posting_invalid_query_yields_error_response() {
        let schema = Arc::new(Mutex::new(Some(Schema {
            id: "test-schema".to_string(),
            document: graphql_parser::parse_schema(
                "\
                 scalar String \
                 type Query { name: String } \
                 ",
            ).unwrap(),
        })));

        let query_runner = Arc::new(TestQueryRunner::default());
        let mut service = GraphQLService::new(schema, query_runner);

        let request = Request::builder()
            .method(Method::POST)
            .uri("http://localhost:8000/graphql")
            .body(Body::from("{}"))
            .unwrap();

        let response = service
            .call(request)
            .wait()
            .expect("Should return a response");
        let errors = test_utils::assert_error_response(response, StatusCode::BAD_REQUEST);

        let message = errors[0]
            .as_object()
            .expect("Query error is not an object")
            .get("message")
            .expect("Error contains no message")
            .as_str()
            .expect("Error message is not a string");

        assert_eq!(message, "The \"query\" field missing in request data");
    }

    #[test]
    fn posting_valid_queries_yields_result_response() {
        let schema = Arc::new(Mutex::new(Some(Schema {
            id: "test-schema".to_string(),
            document: graphql_parser::parse_schema(
                "\
                 scalar String \
                 type Query { name: String } \
                 ",
            ).unwrap(),
        })));

        let query_runner = Arc::new(TestQueryRunner::default());
        let mut service = GraphQLService::new(schema, query_runner);

        let request = Request::builder()
            .method(Method::POST)
            .uri("http://localhost:8000/graphql")
            .body(Body::from("{\"query\": \"{ name }\"}"))
            .unwrap();

        // The response must be a 200
        let response = service
            .call(request)
            .wait()
            .expect("Should return a response");
        let data = test_utils::assert_successful_response(response);

        // The body should match the simulated query result
        let name = data
            .get("name")
            .expect("Query result data has no \"name\" field")
            .as_str()
            .expect("Query result field \"name\" is not a string");
        assert_eq!(name, "Jordi".to_string());
    }
}
