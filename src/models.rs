use diesel::prelude::*;
use facet::Facet;
use facet_format_json::to_vec;
use miette::{IntoDiagnostic, Result};

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
