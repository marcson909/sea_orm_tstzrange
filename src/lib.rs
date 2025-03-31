use std::ops::Bound;
use chrono::{DateTime, Utc};
use sea_orm::{prelude::*, sea_query::{Nullable, SimpleExpr, ValueType, ValueTypeErr}, ColIdx, FromQueryResult, TryGetable, Value};
use sqlx::postgres::types::PgRange;

/// A wrapper for PostgreSQL's tstzrange type
#[derive(Debug, Clone, PartialEq)]
pub struct TstzRange(pub PgRange<DateTime<Utc>>);

impl TstzRange {
    pub fn new(
        start: Bound<DateTime<Utc>>,
        end: Bound<DateTime<Utc>>,
    ) -> Self {
        TstzRange(PgRange { start, end })
    }

    fn to_string(&self) -> String {
        let start = match &self.0.start {
            Bound::Included(dt) => format!("[{}", dt.to_rfc3339()),
            Bound::Excluded(dt) => format!("({}", dt.to_rfc3339()),
            Bound::Unbounded => String::from("("),
        };

        let end = match &self.0.end {
            Bound::Included(dt) => format!("{}]", dt.to_rfc3339()),
            Bound::Excluded(dt) => format!("{})", dt.to_rfc3339()),
            Bound::Unbounded => String::from(")"),
        };

        format!("{},{}", start, end)
    }

    fn from_string(s: &str) -> Result<Self, ValueTypeErr> {
        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() != 2 {
            return Err(ValueTypeErr);
        }

        let start_str = parts[0];
        let end_str = parts[1];

        let start = if start_str == "(" {
            Bound::Unbounded
        } else {
            let inclusive = start_str.starts_with('[');
            let date_str = &start_str[1..];
            let dt = DateTime::parse_from_rfc3339(date_str)
                .map_err(|_| ValueTypeErr)?
                .with_timezone(&Utc);
            if inclusive {
                Bound::Included(dt)
            } else {
                Bound::Excluded(dt)
            }
        };

        let end = if end_str == ")" {
            Bound::Unbounded
        } else {
            let inclusive = end_str.ends_with(']');
            let date_str = &end_str[0..end_str.len()-1];
            let dt = DateTime::parse_from_rfc3339(date_str)
                .map_err(|_| ValueTypeErr)?
                .with_timezone(&Utc);
            if inclusive {
                Bound::Included(dt)
            } else {
                Bound::Excluded(dt)
            }
        };

        Ok(TstzRange(PgRange { start, end }))
    }
}


impl From<TstzRange> for Value {
    fn from(range: TstzRange) -> Self {
        Value::String(Some(Box::new(range.to_string())))
    }
}

impl Nullable for TstzRange {
    fn null() -> Value {
        Value::String(None)
    }
}

impl ValueType for TstzRange {
    fn try_from(v: Value) -> Result<Self, ValueTypeErr> {
        match v {
            Value::String(Some(s)) => Self::from_string(&s),
            _ => Err(ValueTypeErr),
        }
    }

    fn type_name() -> String {
        "tstzrange".to_owned()
    }

    fn array_type() -> sea_orm::sea_query::ArrayType {
        sea_orm::sea_query::ArrayType::String
    }

    fn column_type() -> sea_orm::sea_query::ColumnType {
        sea_orm::sea_query::ColumnType::custom("TSTZRANGE".to_owned())
    }
}



impl TryGetable for TstzRange {
    fn try_get_by<I: ColIdx>(res: &QueryResult, index: I) -> Result<Self, TryGetError> {
        let value = res.try_get_by(index).map_err(TryGetError::DbErr);
        value
    }
}

pub trait RangeOps {
    // @> operator - contains element
    fn contains<T>(&self, element: T) -> SimpleExpr where T: Into<SimpleExpr>;

    // <@ operator - is contained by
    fn contained_by<T>(&self, range: T) -> SimpleExpr where T: Into<SimpleExpr>;

    // && operator - overlaps with
    fn overlaps<T>(&self, range: T) -> SimpleExpr where T: Into<SimpleExpr>;
}

impl RangeOps for Expr {
    fn contains<T>(&self, element: T) -> SimpleExpr where T: Into<SimpleExpr> {
        SimpleExpr::Binary(
            Box::new(self.clone().into()),
            sea_orm::sea_query::BinOper::Custom("@>"),
            Box::new(element.into()),
        )
    }

    fn contained_by<T>(&self, range: T) -> SimpleExpr where T: Into<SimpleExpr> {
        SimpleExpr::Binary(
            Box::new(self.clone().into()),
            sea_orm::sea_query::BinOper::Custom("<@"),
            Box::new(range.into()),
        )
    }

    fn overlaps<T>(&self, range: T) -> SimpleExpr where T: Into<SimpleExpr> {
        SimpleExpr::Binary(
            Box::new(self.clone().into()),
            sea_orm::sea_query::BinOper::Custom("&&"),
            Box::new(range.into()),
        )
    }
}




#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound;

    #[test]
    fn test_to_string_and_from_string() {
        let start = Utc::now();
        let end = start + chrono::Duration::days(1);

        let range = TstzRange::new(
            Bound::Included(start),
            Bound::Excluded(end),
        );

        let string = range.to_string();
        let parsed = TstzRange::from_string(&string).unwrap();

        assert_eq!(range, parsed);
    }

    #[test]
    fn test_unbounded_ranges() {
        // Test with unbounded start
        let end = Utc::now();
        let range1 = TstzRange::new(
            Bound::Unbounded,
            Bound::Excluded(end),
        );

        let string = range1.to_string();
        let parsed = TstzRange::from_string(&string).unwrap();
        assert_eq!(range1, parsed);

        // Test with unbounded end
        let start = Utc::now();
        let range2 = TstzRange::new(
            Bound::Included(start),
            Bound::Unbounded,
        );

        let string = range2.to_string();
        let parsed = TstzRange::from_string(&string).unwrap();
        assert_eq!(range2, parsed);
    }

    #[test]
    fn test_value_conversion() {
        let start = Utc::now();
        let end = start + chrono::Duration::days(1);

        let range = TstzRange::new(
            Bound::Included(start),
            Bound::Excluded(end),
        );

        let value: Value = range.clone().into();
        let restored: TstzRange = <TstzRange as sea_orm::sea_query::ValueType>::try_from(value).unwrap();

        assert_eq!(range, restored);
    }
}
