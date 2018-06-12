use graphql_parser::{query as q, schema as s};
use slog;
use std::collections::{BTreeMap, HashMap};

use thegraph::prelude::*;

use ast::query::object_value;
use ast::schema as sast;
use resolver::Resolver;

type TypeObjectsMap = BTreeMap<String, q::Value>;

fn coerce_scalar_value(t: &s::ScalarType, value: &Option<&q::Value>) -> Option<q::Value> {
    value.and_then(|value| match (t.name.as_str(), value) {
        ("Boolean", v @ q::Value::Boolean(_)) => Some(v.clone()),
        ("Float", v @ q::Value::Float(_)) => Some(v.clone()),
        ("Int", v @ q::Value::Int(_)) => Some(v.clone()),
        ("String", v @ q::Value::String(_)) => Some(v.clone()),
        _ => None,
    })
}

fn coerce_enum_value(t: &s::EnumType, value: &Option<&q::Value>) -> Option<q::Value> {
    value.and_then(|value| match value {
        q::Value::Enum(name) => t.values
            .iter()
            .find(|v| &v.name == name)
            .map(|v| q::Value::Enum(v.name.to_owned())),
        _ => None,
    })
}

fn object_field<'a>(object: &'a Option<q::Value>, field: &str) -> Option<&'a q::Value> {
    object
        .as_ref()
        .and_then(|object| match object {
            q::Value::Object(ref data) => Some(data),
            _ => None,
        })
        .and_then(|data| data.get(field))
}

fn schema_type_objects(schema: &Schema) -> TypeObjectsMap {
    sast::get_type_definitions(&schema.document).iter().fold(
        BTreeMap::new(),
        |mut type_objects, typedef| {
            let type_name = sast::get_type_name(typedef);
            if !type_objects.contains_key(type_name) {
                let type_object = type_definition_object(schema, &mut type_objects, typedef);
                type_objects.insert(type_name.to_owned(), type_object);
            }
            type_objects
        },
    )
}

fn type_object(schema: &Schema, type_objects: &mut TypeObjectsMap, t: &s::Type) -> q::Value {
    match t {
        // We store the name of the named type here to be able to resolve it dynamically later
        s::Type::NamedType(s) => q::Value::String(s.to_owned()),
        s::Type::ListType(ref inner) => list_type_object(schema, type_objects, inner),
        s::Type::NonNullType(ref inner) => non_null_type_object(schema, type_objects, inner),
    }
}

fn named_type_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    name: &s::Name,
) -> q::Value {
    sast::get_named_type(&schema.document, name)
        .map(|typedef| type_definition_object(schema, type_objects, typedef))
        .expect(&format!(
            "Failed to resolve named type in GraphQL schema: {}",
            name
        ))
}

fn list_type_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    inner_type: &s::Type,
) -> q::Value {
    object_value(vec![
        ("kind", q::Value::Enum("LIST".to_string())),
        ("ofType", type_object(schema, type_objects, inner_type)),
    ])
}

fn non_null_type_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    inner_type: &s::Type,
) -> q::Value {
    object_value(vec![
        ("kind", q::Value::Enum("NON_NULL".to_string())),
        ("ofType", type_object(schema, type_objects, inner_type)),
    ])
}

fn type_definition_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    typedef: &s::TypeDefinition,
) -> q::Value {
    let type_name = sast::get_type_name(typedef);

    type_objects
        .get(type_name)
        .map(|type_object| type_object.clone())
        .unwrap_or_else(|| {
            let type_object = match typedef {
                s::TypeDefinition::Enum(enum_type) => enum_type_object(enum_type),
                s::TypeDefinition::InputObject(input_object_type) => {
                    input_object_type_object(schema, type_objects, input_object_type)
                }
                s::TypeDefinition::Interface(interface_type) => {
                    interface_type_object(schema, type_objects, interface_type)
                }
                s::TypeDefinition::Object(object_type) => {
                    object_type_object(schema, type_objects, object_type)
                }
                s::TypeDefinition::Scalar(scalar_type) => scalar_type_object(scalar_type),
                s::TypeDefinition::Union(union_type) => union_type_object(schema, union_type),
                _ => q::Value::Null,
            };

            type_objects.insert(type_name.to_owned(), type_object.clone());
            type_object
        })
}

fn enum_type_object(enum_type: &s::EnumType) -> q::Value {
    object_value(vec![
        ("kind", q::Value::Enum("ENUM".to_string())),
        ("name", q::Value::String(enum_type.name.to_owned())),
        (
            "description",
            enum_type
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        ("enumValues", enum_values(enum_type)),
    ])
}

fn enum_values(enum_type: &s::EnumType) -> q::Value {
    q::Value::List(enum_type.values.iter().map(enum_value).collect())
}

fn enum_value(enum_value: &s::EnumValue) -> q::Value {
    object_value(vec![
        ("name", q::Value::String(enum_value.name.to_owned())),
        (
            "description",
            enum_value
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        ("isDeprecated", q::Value::Boolean(false)),
        ("deprecationReason", q::Value::Null),
    ])
}

fn input_object_type_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    input_object_type: &s::InputObjectType,
) -> q::Value {
    object_value(vec![
        ("name", q::Value::String(input_object_type.name.to_owned())),
        ("kind", q::Value::Enum("INPUT_OBJECT".to_string())),
        (
            "description",
            input_object_type
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        (
            "inputFields",
            input_values(schema, type_objects, &input_object_type.fields),
        ),
    ])
}

fn interface_type_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    interface_type: &s::InterfaceType,
) -> q::Value {
    object_value(vec![
        ("name", q::Value::String(interface_type.name.to_owned())),
        ("kind", q::Value::Enum("INTERFACE".to_string())),
        (
            "description",
            interface_type
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        (
            "fields",
            field_objects(schema, type_objects, &interface_type.fields),
        ),
        (
            "possibleTypes",
            q::Value::List(
                sast::get_object_type_definitions(&schema.document)
                    .iter()
                    .filter(|object_type| {
                        object_type
                            .implements_interfaces
                            .iter()
                            .find(|implemented_name| implemented_name == &&interface_type.name)
                            .is_some()
                    })
                    .map(|object_type| q::Value::String(object_type.name.to_owned()))
                    .collect(),
            ),
        ),
    ])
}

fn object_type_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    object_type: &s::ObjectType,
) -> q::Value {
    type_objects
        .get(&object_type.name)
        .map(|type_object| type_object.clone())
        .unwrap_or_else(|| {
            let type_object = object_value(vec![
                ("kind", q::Value::Enum("OBJECT".to_string())),
                ("name", q::Value::String(object_type.name.to_owned())),
                (
                    "description",
                    object_type
                        .description
                        .as_ref()
                        .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
                ),
                (
                    "fields",
                    field_objects(schema, type_objects, &object_type.fields),
                ),
                (
                    "interfaces",
                    object_interfaces(schema, type_objects, object_type),
                ),
            ]);

            type_objects.insert(object_type.name.to_owned(), type_object.clone());
            type_object
        })
}

fn field_objects(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    fields: &Vec<s::Field>,
) -> q::Value {
    q::Value::List(
        fields
            .into_iter()
            .map(|field| field_object(schema, type_objects, field))
            .collect(),
    )
}

fn field_object(schema: &Schema, type_objects: &mut TypeObjectsMap, field: &s::Field) -> q::Value {
    object_value(vec![
        ("name", q::Value::String(field.name.to_owned())),
        (
            "description",
            field
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        ("args", input_values(schema, type_objects, &field.arguments)),
        ("type", type_object(schema, type_objects, &field.field_type)),
        ("isDeprecated", q::Value::Boolean(false)),
        ("deprecationReason", q::Value::Null),
    ])
}

fn object_interfaces(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    object_type: &s::ObjectType,
) -> q::Value {
    q::Value::List(
        object_type
            .implements_interfaces
            .iter()
            .map(|name| named_type_object(schema, type_objects, name))
            .collect(),
    )
}

fn scalar_type_object(scalar_type: &s::ScalarType) -> q::Value {
    object_value(vec![
        ("name", q::Value::String(scalar_type.name.to_owned())),
        ("kind", q::Value::Enum("SCALAR".to_string())),
        (
            "description",
            scalar_type
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        ("isDeprecated", q::Value::Boolean(false)),
        ("deprecationReason", q::Value::Null),
    ])
}

fn union_type_object(schema: &Schema, union_type: &s::UnionType) -> q::Value {
    object_value(vec![
        ("name", q::Value::String(union_type.name.to_owned())),
        ("kind", q::Value::Enum("UNION".to_string())),
        (
            "description",
            union_type
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        (
            "possibleTypes",
            q::Value::List(
                sast::get_object_type_definitions(&schema.document)
                    .iter()
                    .filter(|object_type| {
                        object_type
                            .implements_interfaces
                            .iter()
                            .find(|implemented_name| implemented_name == &&union_type.name)
                            .is_some()
                    })
                    .map(|object_type| q::Value::String(object_type.name.to_owned()))
                    .collect(),
            ),
        ),
    ])
}

fn schema_directive_objects(schema: &Schema, type_objects: &mut TypeObjectsMap) -> q::Value {
    q::Value::List(
        schema
            .document
            .definitions
            .iter()
            .filter_map(|d| match d {
                s::Definition::DirectiveDefinition(dd) => Some(dd),
                _ => None,
            })
            .map(|dd| directive_object(schema, type_objects, dd))
            .collect(),
    )
}

fn directive_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    directive: &s::DirectiveDefinition,
) -> q::Value {
    object_value(vec![
        ("name", q::Value::String(directive.name.to_owned())),
        (
            "description",
            directive
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        ("locations", directive_locations(directive)),
        (
            "args",
            input_values(schema, type_objects, &directive.arguments),
        ),
    ])
}

fn directive_locations(directive: &s::DirectiveDefinition) -> q::Value {
    q::Value::List(
        directive
            .locations
            .iter()
            .map(|location| location.as_str())
            .map(|name| q::Value::String(name.to_owned()))
            .collect(),
    )
}

fn input_values(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    input_values: &Vec<s::InputValue>,
) -> q::Value {
    q::Value::List(
        input_values
            .iter()
            .map(|value| input_value(schema, type_objects, value))
            .collect(),
    )
}

fn input_value(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    input_value: &s::InputValue,
) -> q::Value {
    object_value(vec![
        ("name", q::Value::String(input_value.name.to_owned())),
        (
            "description",
            input_value
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        (
            "type",
            type_object(schema, type_objects, &input_value.value_type),
        ),
        (
            "defaultValue",
            input_value
                .default_value
                .as_ref()
                .map_or(q::Value::Null, |value| value.clone()),
        ),
    ])
}

#[derive(Clone)]
pub struct IntrospectionResolver<'a> {
    logger: slog::Logger,
    schema: &'a Schema,
    type_objects: TypeObjectsMap,
    directives: q::Value,
}

impl<'a> IntrospectionResolver<'a> {
    pub fn new(logger: &slog::Logger, schema: &'a Schema) -> Self {
        let logger = logger.new(o!("component" => "IntrospectionResolver"));

        // Generate queryable objects for all types in the schema
        let mut type_objects = schema_type_objects(schema);

        // Generate queryable objects for all directives in the schema
        let directives = schema_directive_objects(schema, &mut type_objects);

        IntrospectionResolver {
            logger,
            schema,
            type_objects,
            directives,
        }
    }

    fn schema_object(&self) -> q::Value {
        object_value(vec![
            (
                "queryType",
                self.type_objects
                    .get(&String::from("Query"))
                    .map(|t| t.clone())
                    .unwrap_or(q::Value::Null),
            ),
            ("mutationType", q::Value::Null),
            (
                "types",
                q::Value::List(
                    self.type_objects
                        .values()
                        .map(|t| t.clone())
                        .collect::<Vec<q::Value>>(),
                ),
            ),
            ("directives", self.directives.clone()),
        ])
    }

    fn type_object(&self, arguments: &HashMap<&q::Name, q::Value>) -> q::Value {
        q::Value::Null
    }
}

/// A GraphQL resolver that can resolve entities, enum values, scalar types and interfaces/unions.
impl<'a> Resolver for IntrospectionResolver<'a> {
    fn resolve_entities(
        &self,
        parent: &Option<q::Value>,
        field: &q::Name,
        entity: &q::Name,
        arguments: &HashMap<&q::Name, q::Value>,
    ) -> q::Value {
        debug!(self.logger, "Resolve entities"; "field" => field);

        match field.as_str() {
            "possibleTypes" => {
                let type_names = object_field(parent, "possibleTypes")
                    .cloned()
                    .and_then(|value| match value {
                        q::Value::List(type_names) => Some(type_names),
                        _ => None,
                    })
                    .unwrap_or(vec![]);

                if type_names.len() > 0 {
                    q::Value::List(
                        type_names
                            .iter()
                            .filter_map(|type_name| match type_name {
                                q::Value::String(ref type_name) => Some(type_name),
                                _ => None,
                            })
                            .map(|type_name| {
                                named_type_object(&self.schema, &mut self.type_objects, type_name)
                            })
                            .collect(),
                    )
                } else {
                    q::Value::Null
                }
            }
            _ => {
                let value = parent
                    .as_ref()
                    .and_then(|parent| match parent {
                        q::Value::Object(object) => object.get(field),
                        _ => None,
                    })
                    .map(|value| value.clone())
                    .unwrap_or(q::Value::Null);

                debug!(self.logger, "Resolve entities"; "result" => format!("{:#?}", value));
                value
            }
        }
    }

    fn resolve_entity(
        &self,
        parent: &Option<q::Value>,
        field: &q::Name,
        entity: &q::Name,
        arguments: &HashMap<&q::Name, q::Value>,
    ) -> q::Value {
        match field.as_str() {
            "__schema" => self.schema_object(),
            "__type" => self.type_object(arguments),
            "type" => object_field(parent, "type")
                .map(|value| match value {
                    q::Value::String(ref type_name) => {
                        named_type_object(&self.schema, &mut self.type_objects, type_name)
                    }
                    _ => value.clone(),
                })
                .unwrap_or(q::Value::Null),
            "ofType" => object_field(parent, "ofType")
                .map(|value| match value {
                    q::Value::String(ref type_name) => {
                        named_type_object(&self.schema, &mut self.type_objects, type_name)
                    }
                    _ => value.clone(),
                })
                .unwrap_or(q::Value::Null),
            _ => parent
                .as_ref()
                .and_then(|parent| match parent {
                    q::Value::Object(object) => object.get(field),
                    _ => None,
                })
                .map(|value| value.clone())
                .unwrap_or(q::Value::Null),
        }
    }

    fn resolve_enum_value(
        &self,
        enum_type: &s::EnumType,
        value: Option<&q::Value>,
    ) -> q::Value {
        coerce_enum_value(enum_type, &value).unwrap_or(q::Value::Null)
    }

    fn resolve_scalar_value(
        &self,
        scalar_type: &s::ScalarType,
        value: Option<&q::Value>,
    ) -> q::Value {
        coerce_scalar_value(scalar_type, &value).unwrap_or(q::Value::Null)
    }

    fn resolve_enum_values(
        &self,
        enum_type: &s::EnumType,
        value: Option<&q::Value>,
    ) -> q::Value {
        value
            .and_then(|value| match value {
                q::Value::List(values) => {
                    let coerced_values: Vec<q::Value> = values
                        .iter()
                        .filter_map(|value| coerce_enum_value(enum_type, &Some(value)))
                        .collect();

                    if values.len() == coerced_values.len() {
                        Some(q::Value::List(coerced_values))
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .unwrap_or(q::Value::Null)
    }

    fn resolve_scalar_values(
        &self,
        scalar_type: &s::ScalarType,
        value: Option<&q::Value>,
    ) -> q::Value {
        value
            .and_then(|value| match value {
                q::Value::List(values) => {
                    let coerced_values: Vec<q::Value> = values
                        .iter()
                        .filter_map(|value| coerce_scalar_value(scalar_type, &Some(value)))
                        .collect();

                    if values.len() == coerced_values.len() {
                        Some(q::Value::List(coerced_values))
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .unwrap_or(q::Value::Null)
    }

    fn resolve_abstract_type<'b>(
        &self,
        schema: &'b s::Document,
        abstract_type: &s::TypeDefinition,
        object_value: &q::Value,
    ) -> Option<&'b s::ObjectType> {
        None
    }
}