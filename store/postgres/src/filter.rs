use std::str::FromStr;

use bigdecimal::BigDecimal;
use db_schema::entities;
use diesel::dsl::{not, sql};
use diesel::expression::NonAggregate;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::{BoxedSelectStatement, QueryFragment};
use diesel::sql_types::{Bool, Float, Integer, Jsonb, Numeric, Text};
use diesel::AppearsOnTable;

use thegraph::components::store::StoreFilter;
use thegraph::data::store::*;

use serde_json;

pub(crate) struct UnsupportedFilter {
    pub filter: String,
    pub value: Value,
}

enum FilterMode {
    And,
    Or,
}

/// Adds `filter` to a `SELECT data FROM entities` statement.
pub(crate) fn store_filter<'a>(
    query: BoxedSelectStatement<'a, Jsonb, entities::table, Pg>,
    filter: StoreFilter,
) -> Result<BoxedSelectStatement<'a, Jsonb, entities::table, Pg>, UnsupportedFilter> {
    store_filter_by_mode(query, filter, FilterMode::And)
}

fn add_filter<'a, P: 'a>(
    query: BoxedSelectStatement<'a, Jsonb, entities::table, Pg>,
    filter_mode: FilterMode,
    predicate: P,
) -> BoxedSelectStatement<'a, Jsonb, entities::table, Pg>
where
    P: AppearsOnTable<entities::table>
        + NonAggregate
        + QueryFragment<Pg>
        + Expression<SqlType = Bool>,
{
    match filter_mode {
        FilterMode::And => query.filter(predicate),
        FilterMode::Or => query.or_filter(predicate),
    }
}

/// Adds `filter` to a `SELECT data FROM entities` statement.
fn store_filter_by_mode<'a>(
    query: BoxedSelectStatement<'a, Jsonb, entities::table, Pg>,
    filter: StoreFilter,
    filter_mode: FilterMode,
) -> Result<BoxedSelectStatement<'a, Jsonb, entities::table, Pg>, UnsupportedFilter> {
    Ok(match filter {
        StoreFilter::And(filters) => filters
            .into_iter()
            .try_fold(query, |q, f| store_filter_by_mode(q, f, FilterMode::And))?,
        StoreFilter::Or(filters) => filters
            .into_iter()
            .try_fold(query, |q, f| store_filter_by_mode(q, f, FilterMode::Or))?,
        StoreFilter::Contains(attribute, value) => match value {
            Value::String(query_value) => add_filter(
                query,
                filter_mode,
                sql("data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(" LIKE ")
                    .bind::<Text, _>(query_value),
            ),
            Value::Bytes(query_value) => add_filter(
                query,
                filter_mode,
                sql("data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(" LIKE ")
                    .bind::<Text, _>(query_value.to_string()),
            ),
            Value::List(query_value) => {
                let query_array =
                    serde_json::to_string(&query_value).expect("Failed to serialize Value");
                add_filter(
                    query,
                    filter_mode,
                    // Is `query_array` contained in array `data ->> attribute`?
                    sql("data ->> ")
                        .bind::<Text, _>(attribute)
                        .sql(" @> ")
                        .bind::<Text, _>(query_array),
                )
            }
            Value::Null | Value::Float(_) | Value::Int(_) | Value::Bool(_) | Value::BigInt(_) => {
                return Err(UnsupportedFilter {
                    filter: "contains".to_owned(),
                    value,
                })
            }
        },
        StoreFilter::Equal(..) | StoreFilter::Not(..) => {
            let (attribute, op, value) = match filter {
                StoreFilter::Equal(attribute, value) => (attribute, "=", value),
                StoreFilter::Not(attribute, value) => (attribute, "!=", value),
                _ => unreachable!(),
            };

            match value {
                Value::String(query_value) => add_filter(
                    query,
                    filter_mode,
                    sql("(")
                        .sql("data ->> ")
                        .bind::<Text, _>(attribute)
                        .sql(")")
                        .sql(op)
                        .bind::<Text, _>(query_value),
                ),
                Value::Float(query_value) => add_filter(
                    query,
                    filter_mode,
                    sql("(")
                        .sql("data ->> ")
                        .bind::<Text, _>(attribute)
                        .sql(")")
                        .sql("::float")
                        .sql(op)
                        .bind::<Float, _>(query_value),
                ),
                Value::Int(query_value) => add_filter(
                    query,
                    filter_mode,
                    sql("(data ->> ")
                        .bind::<Text, _>(attribute)
                        .sql(")")
                        .sql("::int")
                        .sql(op)
                        .bind::<Integer, _>(query_value),
                ),
                Value::Bool(query_value) => add_filter(
                    query,
                    filter_mode,
                    sql("(data ->> ")
                        .bind::<Text, _>(attribute)
                        .sql(")")
                        .sql("::boolean")
                        .sql(op)
                        .bind::<Bool, _>(query_value),
                ),
                Value::Null => add_filter(
                    query,
                    filter_mode,
                    sql("data -> ").bind::<Text, _>(attribute).sql(" = 'null' "),
                ),
                Value::List(query_value) => {
                    // Note that lists with the same elements but in different order
                    // are considered not equal.
                    let query_array =
                        serde_json::to_string(&query_value).expect("Failed to serialize Value");
                    add_filter(
                        query,
                        filter_mode,
                        sql("data ->> ")
                            .bind::<Text, _>(attribute)
                            .sql(op)
                            .bind::<Text, _>(query_array),
                    )
                }
                Value::Bytes(query_value) => {
                    let hex_string =
                        serde_json::to_string(&query_value).expect("Failed to serialize Value");
                    add_filter(
                        query,
                        filter_mode,
                        sql("(data ->> ")
                            .bind::<Text, _>(attribute)
                            .sql(op)
                            .bind::<Text, _>(hex_string),
                    )
                }
                Value::BigInt(query_value) => add_filter(
                    query,
                    filter_mode,
                    sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                .sql(")")
                .sql("::numeric")
                .sql(op)
                // Using `BigDecimal::new(query_value.0, 0)` results in a
                // mismatch of `bignum` versions, go through the string
                // representation to work around that.
                .bind::<Numeric, _>(BigDecimal::from_str(&query_value.to_string()).unwrap()),
                ),
            }
        }
        StoreFilter::GreaterThan(..)
        | StoreFilter::LessThan(..)
        | StoreFilter::GreaterOrEqual(..)
        | StoreFilter::LessOrEqual(..) => {
            let (attribute, op, value) = match filter {
                StoreFilter::GreaterThan(attribute, value) => (attribute, ">", value),
                StoreFilter::LessThan(attribute, value) => (attribute, "<", value),
                StoreFilter::GreaterOrEqual(attribute, value) => (attribute, ">=", value),
                StoreFilter::LessOrEqual(attribute, value) => (attribute, "<=", value),
                _ => unreachable!(),
            };
            match value {
                Value::String(query_value) => add_filter(
                    query,
                    filter_mode,
                    sql("data ->> ")
                        .bind::<Text, _>(attribute)
                        .sql(op)
                        .bind::<Text, _>(query_value),
                ),
                Value::Float(query_value) => add_filter(
                    query,
                    filter_mode,
                    sql("(data ->> ")
                        .bind::<Text, _>(attribute)
                        .sql(")")
                        .sql("::float")
                        .sql(op)
                        .bind::<Float, _>(query_value as f32),
                ),
                Value::Int(query_value) => add_filter(
                    query,
                    filter_mode,
                    sql("(data ->> ")
                        .bind::<Text, _>(attribute)
                        .sql(")")
                        .sql("::int")
                        .sql(op)
                        .bind::<Integer, _>(query_value),
                ),
                Value::BigInt(query_value) => add_filter(
                    query,
                    filter_mode,
                    sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                .sql(")")
                .sql("::numeric")
                .sql(op)
                // Using `BigDecimal::new(query_value.0, 0)` results in a
                // mismatch of `bignum` versions, go through the string
                // representation to work around that.
                .bind::<Numeric, _>(BigDecimal::from_str(&query_value.to_string()).unwrap()),
                ),
                Value::Null | Value::Bool(_) | Value::List(_) | Value::Bytes(_) => {
                    return Err(UnsupportedFilter {
                        filter: op.to_owned(),
                        value,
                    })
                }
            }
        }
        StoreFilter::NotContains(attribute, value) => match value {
            Value::String(query_value) => add_filter(
                query,
                filter_mode,
                sql("data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(" NOT LIKE ")
                    .bind::<Text, _>(query_value),
            ),
            Value::Bytes(query_value) => add_filter(
                query,
                filter_mode,
                sql("data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(" NOT LIKE ")
                    .bind::<Text, _>(query_value.to_string()),
            ),
            Value::List(query_value) => {
                let query_array =
                    serde_json::to_string(&query_value).expect("Failed to serialize Value");
                add_filter(
                    query,
                    filter_mode,
                    // Is `query_array` _not_ contained in array `data ->> attribute`?
                    not(sql("data ->> ")
                        .bind::<Text, _>(attribute)
                        .sql(" @> ")
                        .bind::<Text, _>(query_array)),
                )
            }
            Value::Null | Value::Float(_) | Value::Int(_) | Value::Bool(_) | Value::BigInt(_) => {
                return Err(UnsupportedFilter {
                    filter: "not_contains".to_owned(),
                    value,
                })
            }
        },

        // We will add support for more filters later
        _ => unimplemented!(),
    })
}