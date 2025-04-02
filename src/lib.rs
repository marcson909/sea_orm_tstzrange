use std::{fmt::Display, ops::Bound};
use chrono::{DateTime, Utc};
use sea_orm::{prelude::*, sea_query::{Nullable, SimpleExpr, ValueType, ValueTypeErr}, ColIdx, TryGetable, Value};
use serde::{Deserialize, Serialize};
use sqlx::postgres::types::PgRange;
use anyhow::anyhow;

#[derive(Debug, Clone, PartialEq)]
pub struct TstzRange(pub PgRange<DateTime<Utc>>);

impl TstzRange {
    pub fn new(
        start: Bound<DateTime<Utc>>,
        end: Bound<DateTime<Utc>>,
    ) -> Self {
        TstzRange(PgRange { start, end })
    }

    #[inline(always)]
    fn clean_and_parse(date_str: &str) -> Result<DateTime<Utc>, ValueTypeErr> {
        let s = date_str.strip_prefix("\"").unwrap_or(date_str);
        let s = s.strip_suffix("\"").unwrap_or(s);

        let s = if s.matches('+').count() == 1 && s.ends_with("+00") {
            format!("{}:00", s)
        } else {
            s.to_string()
        };
        s.parse::<DateTime<Utc>>()
        .inspect_err(|e| eprintln!("failed to parse dt: {e}"))
        .map_err(|_| ValueTypeErr)
    }

    fn _parse_bound<T>(ch: char, value: Option<T>) -> Result<Bound<T>, anyhow::Error> {
        Ok(if let Some(value) = value {
            match ch {
                '(' | ')' => Bound::Excluded(value),
                '[' | ']' => Bound::Included(value),

                _ => {

                    return Err(anyhow!(
                        "expected `(`, ')', '[', or `]` but found `{ch}` for range literal"
                    )
                    );
                }
            }
        } else {
            Bound::Unbounded
        })
    }

    pub fn from_string(s: &str) -> Result<Self, ValueTypeErr> {
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
            let dt = Self::clean_and_parse(date_str)?;

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
            let dt = Self::clean_and_parse(date_str)?;

            if inclusive {
                Bound::Included(dt)
            } else {
                Bound::Excluded(dt)
            }
        };

        Ok(TstzRange(PgRange { start, end }))
    }


    pub fn from_datetime_pair(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self::new(Bound::Included(start), Bound::Excluded(end))
    }


    pub fn contains_timestamp(&self, timestamp: &DateTime<Utc>) -> bool {
        match (&self.0.start, &self.0.end) {
            (Bound::Included(start), Bound::Included(end)) => timestamp >= start && timestamp <= end,
            (Bound::Included(start), Bound::Excluded(end)) => timestamp >= start && timestamp < end,
            (Bound::Excluded(start), Bound::Included(end)) => timestamp > start && timestamp <= end,
            (Bound::Excluded(start), Bound::Excluded(end)) => timestamp > start && timestamp < end,
            (Bound::Included(start), Bound::Unbounded) => timestamp >= start,
            (Bound::Excluded(start), Bound::Unbounded) => timestamp > start,
            (Bound::Unbounded, Bound::Included(end)) => timestamp <= end,
            (Bound::Unbounded, Bound::Excluded(end)) => timestamp < end,
            (Bound::Unbounded, Bound::Unbounded) => true,
        }
    }

    pub fn start(&self) -> Option<DateTime<Utc>> {
        match &self.0.start {
            Bound::Included(dt) | Bound::Excluded(dt) => Some(*dt),
            Bound::Unbounded => None,
        }
    }

    pub fn end(&self) -> Option<DateTime<Utc>> {
        match &self.0.end {
            Bound::Included(dt) | Bound::Excluded(dt) => Some(*dt),
            Bound::Unbounded => None,
        }
    }

    // Check if start/end are inclusive
    pub fn is_start_inclusive(&self) -> bool {
        matches!(&self.0.start, Bound::Included(_))
    }

    pub fn is_end_inclusive(&self) -> bool {
        matches!(&self.0.end, Bound::Included(_))
    }
}



impl Display for TstzRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let start = match &self.0.start {
            Bound::Included(v) => format!("[{}", v.to_rfc3339()),
            Bound::Excluded(v) => format!("({}", v.to_rfc3339()),
            Bound::Unbounded => "(".to_string(),
        };
        let end = match &self.0.end {
            Bound::Included(v) => format!("{}]", v.to_rfc3339()),
            Bound::Excluded(v) => format!("{})", v.to_rfc3339()),
            Bound::Unbounded => ")".to_string(),
        };
        write!(f, "{},{}", start, end)
    }
}


impl Serialize for TstzRange {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer
    {

        (&self.0.start, &self.0.end).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TstzRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de> {
        let (start, end) = Deserialize::deserialize(deserializer)?;
        Ok(TstzRange(PgRange{ start, end }))
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
    fn try_get_by<I: ColIdx>(res: &QueryResult, idx: I) -> Result<Self, TryGetError> {
        let value = res.try_get_by::<Option<String>, I>(idx)?.into();
        match value {
            Value::String(Some(s)) => {
                let range = TstzRange::from_string(&s)
                    .map_err(|e| TryGetError::Null(e.to_string()))?;
                Ok(range)
            }
            _ => Err(TryGetError::Null("Unexpected value type".to_string())),
        }
    }
}

impl From<PgRange<DateTime<Utc>>> for TstzRange {
    fn from(range: PgRange<DateTime<Utc>>) -> Self {
        TstzRange(range)
    }
}

impl From<TstzRange> for PgRange<DateTime<Utc>> {
    fn from(range: TstzRange) -> Self {
        range.0
    }
}

impl From<TstzRange> for Value {
    fn from(x: TstzRange) -> Value {
        let v = Value::String(Some(Box::new(x.to_string())));
        v
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

        let end = Utc::now();
        let range1 = TstzRange::new(
            Bound::Unbounded,
            Bound::Excluded(end),
        );

        let string = range1.to_string();
        let parsed = TstzRange::from_string(&string).unwrap();
        assert_eq!(range1, parsed);

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

    #[test]
    fn test_display() {
        let start = Utc::now();
        let end = start + chrono::Duration::days(1);
        let range = TstzRange::new(Bound::Included(start), Bound::Excluded(end));

        let display_str = format!("{}", range);
        assert!(!display_str.is_empty());
        assert!(display_str.contains(&start.to_rfc3339()));
        assert!(display_str.contains(&end.to_rfc3339()));
    }

    #[test]
    fn test_contains_timestamp() {
        let start = Utc::now();
        let middle = start + chrono::Duration::hours(12);
        let end = start + chrono::Duration::days(1);

        let range = TstzRange::new(Bound::Included(start), Bound::Excluded(end));

        assert!(range.contains_timestamp(&start));
        assert!(range.contains_timestamp(&middle));
        assert!(!range.contains_timestamp(&end));
    }

}

