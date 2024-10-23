use datafusion::common::plan_err;
use datafusion::error::Result;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion::sql::unparser::plan_to_sql;
use std::fs;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let required_tables = vec![
        "customer", "orders", "lineitem", "nation", "partsupp", "supplier", "part", "region",
    ];
    // register the TPC-H tables
    for table in required_tables {
        ctx.register_parquet(
            table,
            &format!("tpch/data/{table}.parquet"),
            ParquetReadOptions::default(),
        )
        .await?;
    }

    for query in 1..=22 {
        println!("#### Running query {} #####", query);
        let sql = get_query_sql(query)?;
        let start = Instant::now();
        // parsing and planning
        let df = ctx.sql(&sql).await?;
        let logical_plan = df.into_unoptimized_plan();
        // converting Plan to SQL
        let result = plan_to_sql(&logical_plan)?;
        let elapsed = start.elapsed();
        println!("input SQL: {sql}");
        println!("----------------------------------");
        println!("logical plan: {logical_plan}");
        println!("----------------------------------");
        println!("output SQL: {result}");
        println!("----------------------------------");
        println!(
            "query {query} took {:.2} ms",
            elapsed.as_secs_f64() * 1000.0
        );
        println!("#### Finished query {} #####", query);
    }

    Ok(())
}

/// Get the SQL statements from the specified query file
pub fn get_query_sql(query: usize) -> Result<String> {
    if query > 0 && query < 23 {
        let filename = format!("tpch/sql/q{query}.sql");
        let mut errors = vec![];
        match fs::read_to_string(&filename) {
            Ok(contents) => {
                return Ok(contents);
            }
            Err(e) => errors.push(format!("{filename}: {e}")),
        };
        plan_err!("invalid query. Could not find query: {:?}", errors)
    } else {
        plan_err!("invalid query. Expected value between 1 and 22")
    }
}
