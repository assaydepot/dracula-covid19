use parquet::record::{RecordSchema, RecordWriter};

#[derive(Debug, ParquetRecordWriter, ParquetRecordSchema)]
pub struct CovidRecord {
    pub status: String,
    pub province_state: Option<String>,
    pub state: Option<String>,
    pub city: Option<String>,
    pub county: Option<String>,
    pub country_region: String,
    pub lat: Option<f32>,
    pub long: Option<f32>,
    pub date: chrono::NaiveDateTime,
    pub count: i64,
}
