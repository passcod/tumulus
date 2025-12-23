use diesel::AsExpression;
use diesel::deserialize::{self, FromSql};
use diesel::prelude::*;
use diesel::serialize::{self, Output, ToSql};
use diesel::sql_types::Text;
use facet::Facet;
use facet_format_json::to_vec;
use jiff::Timestamp;
use miette::{IntoDiagnostic, Result};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, AsExpression)]
#[diesel(sql_type = Text)]
pub struct DbTimestamp(pub Timestamp);

impl fmt::Display for DbTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<DB> ToSql<Text, DB> for DbTimestamp
where
    DB: diesel::backend::Backend,
    String: ToSql<Text, DB>,
{
    fn to_sql(&self, _out: &mut Output<DB>) -> serialize::Result {
        todo!()
        // let s = self.0.to_string();
        // s.to_sql(out)
    }
}

impl<DB> FromSql<Text, DB> for DbTimestamp
where
    DB: diesel::backend::Backend,
    String: FromSql<Text, DB>,
{
    fn from_sql(
        value: <DB as diesel::backend::Backend>::RawValue<'_>,
    ) -> deserialize::Result<Self> {
        let string_value = <String as FromSql<Text, DB>>::from_sql(value)?;
        let ts = string_value.parse::<Timestamp>().map_err(|e| {
            Box::<dyn std::error::Error + Send + Sync>::from(format!(
                "Failed to parse timestamp: {e}"
            ))
        })?;
        Ok(DbTimestamp(ts))
    }
}

#[derive(Queryable, Insertable, Selectable)]
#[diesel(table_name = crate::schema::metadata)]
pub struct Metadata {
    pub key: String,
    pub value: Vec<u8>,
}

impl Metadata {
    pub fn new<'facet, T>(key: &str, value: &T) -> Result<Self>
    where
        T: Facet<'facet>,
    {
        Ok(Self {
            key: key.to_string(),
            value: to_vec(value).into_diagnostic()?,
        })
    }
}

#[derive(Queryable, Insertable, Selectable)]
#[diesel(table_name = crate::schema::extents)]
pub struct Extent {
    pub blob_id: Vec<u8>,
    pub extent_id: Vec<u8>,
    pub offset: i64,
    pub bytes: i64,
}

#[derive(Queryable, Insertable, Selectable)]
#[diesel(table_name = crate::schema::blobs)]
pub struct Blob {
    pub blob_id: Vec<u8>,
    pub bytes: i64,
    pub extents: i64,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = crate::schema::files)]
pub struct File {
    pub file_id: i64,
    pub path: Vec<u8>,
    pub blob_id: Option<Vec<u8>>,
    pub ts_created: Option<DbTimestamp>,
    pub ts_changed: Option<DbTimestamp>,
    pub ts_modified: Option<DbTimestamp>,
    pub ts_accessed: Option<DbTimestamp>,
    pub attributes: Option<Vec<u8>>,
    pub unix_mode: Option<i32>,
    pub unix_owner_id: Option<i32>,
    pub unix_owner_name: Option<String>,
    pub unix_group_id: Option<i32>,
    pub unix_group_name: Option<String>,
    pub special: Option<Vec<u8>>,
    pub extra: Option<Vec<u8>>,
}

#[derive(Insertable)]
#[diesel(table_name = crate::schema::files)]
pub struct NewFile {
    pub path: Vec<u8>,
    pub blob_id: Option<Vec<u8>>,
    pub ts_created: Option<DbTimestamp>,
    pub ts_changed: Option<DbTimestamp>,
    pub ts_modified: Option<DbTimestamp>,
    pub ts_accessed: Option<DbTimestamp>,
    pub attributes: Option<Vec<u8>>,
    pub unix_mode: Option<i32>,
    pub unix_owner_id: Option<i32>,
    pub unix_owner_name: Option<String>,
    pub unix_group_id: Option<i32>,
    pub unix_group_name: Option<String>,
    pub special: Option<Vec<u8>>,
    pub extra: Option<Vec<u8>>,
}
