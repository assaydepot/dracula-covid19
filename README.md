Dracula: COVID-19
===

Dracula-COVID19 is a remix of Scientist's internal `dracula` tooling. This is an ETL tool for safely transforming COVID-19 data from untyped CSV to typed Parquet suitable for cloudnative tools like AWS Athena (Presto+HIVE). We remap data to optional (nullable) columns and convert dates to native date types so they are easy to analyze in SQL (without having to fight the AWS Glue crawler to properly recognize dates).

How does it work?
---

 1. Load the [Johns Hopkins CSSE team's CSV data](https://github.com/CSSEGISandData/COVID-19) in to memory (we combine all 3 statuses: _confirmed_, _deaths_, _recovered_)
 2. Pivot the date columns to rows
 3. Normalize US state names (convert abbrevations)
 4. Normalize French territories (lat/lon pairs will compute a numeric mean in tableau, which is undesireable)
 5. Write out parquet
 6. Upload to S3 (currently a private bucket, sorry!)
 7. Triggers a AWS Glue crawler to rebuild the Hive schema for the table
 
How do I run it?
---

```
git clone https://github.com/assaydepot/dracula-covid19.git
cd dracula-covid19
cargo run 
``` 

the tool is not optimized for local use but we're open to PRs to make it easier (probably just a matter of skipping the AWS cloud API calls). Right now we're focused on the data but we're open to PRs!

What libraries does it use?
---

 * The great native Rust AWS SDK: rusoto
 * Reqwest for reading CSVs
 * Tokio for async
 * The great native Rust Parquet library: parquet-rs
 * CSV