use std::collections::HashMap;
use std::env;
use anyhow::{Result, anyhow};
use csv::{ReaderBuilder, WriterBuilder, Trim};
use chrono::{NaiveDateTime, NaiveDate, DateTime};

#[derive(Debug)]
enum DataType {
    Varchar(usize),
    Integer,
    Float,
    Boolean,
    Timestamp,
    Timestamptz,
    Date
}

//Parse Datatype names filled out on the mapping csv
//Mapping csv's columns are: field_name, data_type
//These datatypes are arbitary SQL style datatypes
fn parse_data_type(s: &str) -> Result<DataType> {
    
    //Check if varchar and if so attempt to parse the length
    if let Some(l) = s.strip_prefix("varchar(").and_then(|r| r.strip_suffix(')')) {
        Ok(DataType::Varchar(l.parse()?))
    }
    //check if integer
    else if s.eq_ignore_ascii_case("integer") {
        Ok(DataType::Integer)
    }
    //check if float
    else if s.eq_ignore_ascii_case("float") {
        Ok(DataType::Float)
    }
    //check if boolean
    else if s.eq_ignore_ascii_case("boolean") {
        Ok(DataType::Boolean)
    }
    //check if timestamp
    else if s.eq_ignore_ascii_case("timestamp") {
        Ok(DataType::Timestamp)
    }
    //check if timestamptz
    else if s.eq_ignore_ascii_case("timestamptz") {
        Ok(DataType::Timestamptz)
    }
    //check if date
    else if s.eq_ignore_ascii_case("date") {
        Ok(DataType::Date)
    }
    // if datatype in mapping doesn't match any of the above throw an error
    else {
        Err(anyhow!("Unknown data type: {}", s))
    }
}

fn main() -> Result<()> {
    
    // Expect exactly two CLI arguments: mapping CSV and input CSV
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <mapping.csv> <input.csv>", args[0]);
        std::process::exit(1);
    }
    let mapping_path = &args[1];
    let input_path = &args[2];
    
    // Load mapping (schema) CSV
    let mut rdr = ReaderBuilder::new()
        .trim(Trim::All) //Trim whitespace trailing and leading just in case any issues
        .from_path(mapping_path)?;
    let mut mapping = HashMap::new(); //use HashMap type for mapping for quick look up speed
    let mut header_order = Vec::new(); //add header_order variable to preserve order of headers
    
    //iterate over records in the mapping table
    for result in rdr.records() {
        let record = result?;
        let field_name = record[0].to_string();
        let data_type = parse_data_type(&record[1])?;
        //after parsing add record to the mapping HashMap
        mapping.insert(record[0].to_string(), data_type);
        //finally add field name to the header order Vec in order to preserve ordanance
        header_order.push(field_name);
    }

    // Open input CSV and prepare CSV writer for stdout
    let mut input = ReaderBuilder::new()
        .trim(Trim::All)
        .from_path(input_path)?;

    let mut wtr = WriterBuilder::new()
        .from_writer(std::io::stdout());

    // Write output header row using header_order
    // Preserve order by collecting field names from header_order Vec
    let headers: Vec<&String> = header_order.iter().collect();
    wtr.write_record(headers.iter().map(|s| s.as_str()))?;

    // Get input headers once for indexing fields
    let input_headers = input.headers()?.clone();
    
    //iterate through records in the csv
    for result in input.records() {
        let record = result?;
        let mut output_row = Vec::with_capacity(mapping.len()); //init row with mem capacity the
                                                                //same length as mapping
        
        //iterate over each field in headers
        for field in &headers {

            // Find index of the field in input record (may not exist)
            // Attempts to find header in input file based on position in header_order
            if let Some(idx) = input_headers.iter().position(|h| h == *field) {
                //gets record value at position
                let val = record.get(idx).unwrap_or("");
                //gets datatype from mapping using field
                let dtype = mapping.get(*field).unwrap();

                //transform functions for value according to datatype
                let transformed = match dtype {
                    //Attempts convert value to Varchar if dt is varchar and truncates to length
                    DataType::Varchar(max_len) => val.chars().take(*max_len).collect(),
                    //Attempts convert value to Integer by first converting to float and then
                    //rounding (worth noting that float size is higher to allow for the largest
                    //ints in i32
                    DataType::Integer => val.parse::<f64>()
                        .map(|v| (v.round() as i32).to_string())
                        .unwrap_or_else(|_| "".to_string()),
                    //Attempts to convert val to float
                    DataType::Float => val.parse::<f32>()
                        .map(|v| v.to_string())
                        .unwrap_or_else(|_| "".to_string()),
                    //Attempts to parse as bool if dt is boolean
                    DataType::Boolean => {
                        let normalized = val.trim().to_lowercase();
                        let parsed = match normalized.as_str() {
                            "true" | "yes" | "1" => Some(true),
                            "false" | "no" | "0" => Some(false),
                            _ => val.parse::<bool>().ok(),
                        };
                        parsed
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "".to_string())
                    },
                    //Attempts to parse as datetime if dt is timestamp
                    DataType::Timestamp => {
                        NaiveDateTime::parse_from_str(val, "%Y-%m-%d %H:%M:%S")
                            .map(|dt| dt.to_string())
                            .unwrap_or_else(|_| "".to_string())
                    },
                    //Attempts to parse as datetime if dt is timestamptz
                    DataType::Timestamptz => {
                        DateTime::parse_from_str(val, "%Y-%m-%d %H:%M:%S%.f %:z")
                            .map(|dtz| dtz.format("%Y-%m-%d %H:%M:%S%.f %:z").to_string())
                            .unwrap_or_else(|_| {
                                val.parse::<i64>().ok()
                                    .and_then(|ts| DateTime::from_timestamp(ts, 0))
                                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.f %:z").to_string())
                                    .unwrap_or_else(|| "".to_string())
                            })
                    },
                    //Attempts to parse as date if dt is date
                    DataType::Date => {
                        NaiveDate::parse_from_str(val, "%Y-%m-%d")
                            .map(|d| d.to_string())
                            .unwrap_or_else(|_| "".to_string())
                    }
                };
                //Add the transformed val to the output row
                output_row.push(transformed);
            } else {
                // Field not found in input; push empty field
                output_row.push(String::new());
            }
        }
        //write the row to stdout
        wtr.write_record(&output_row)?;
    }
    
    //clear stdout
    wtr.flush()?;
    Ok(())
}

