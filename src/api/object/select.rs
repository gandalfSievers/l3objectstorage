//! SelectObjectContent operations
//!
//! S3 Select allows SQL queries against CSV and JSON objects.
//! Supports basic SELECT queries with WHERE clause, column selection, LIMIT, and aggregates.

use bytes::{Bytes, BytesMut};
use http_body_util::Full;
use hyper::Response;
use serde_json::{Map, Value as JsonValue};
use std::collections::HashMap;

use crate::storage::StorageEngine;
use crate::types::error::{S3Error, S3ErrorCode, S3Result};

// =============================================================================
// Request Types
// =============================================================================

#[derive(Debug, Clone)]
pub struct SelectRequest {
    pub expression: String,
    pub input_serialization: InputSerialization,
    pub output_serialization: OutputSerialization,
}

#[derive(Debug, Clone)]
pub struct InputSerialization {
    pub format: InputFormat,
    pub compression_type: CompressionType,
}

#[derive(Debug, Clone)]
pub enum InputFormat {
    Csv(CsvInput),
    Json(JsonInput),
}

#[derive(Debug, Clone, Default)]
pub struct CsvInput {
    pub file_header_info: FileHeaderInfo,
    pub field_delimiter: char,
    #[allow(dead_code)]
    pub record_delimiter: char,
    #[allow(dead_code)]
    pub quote_character: char,
}

#[derive(Debug, Clone, Default)]
pub enum FileHeaderInfo {
    Use,
    Ignore,
    #[default]
    None,
}

#[derive(Debug, Clone)]
pub struct JsonInput {
    pub json_type: JsonType,
}

#[derive(Debug, Clone, Default)]
pub enum JsonType {
    #[default]
    Document,
    Lines,
}

#[derive(Debug, Clone, Default)]
pub enum CompressionType {
    #[default]
    None,
    Gzip,
    Bzip2,
}

#[derive(Debug, Clone)]
pub struct OutputSerialization {
    pub format: OutputFormat,
}

#[derive(Debug, Clone)]
pub enum OutputFormat {
    Csv(CsvOutput),
    Json(JsonOutput),
}

#[derive(Debug, Clone, Default)]
pub struct CsvOutput {
    pub field_delimiter: char,
    pub record_delimiter: char,
    #[allow(dead_code)]
    pub quote_character: char,
}

#[derive(Debug, Clone, Default)]
pub struct JsonOutput {
    pub record_delimiter: char,
}

// =============================================================================
// SQL AST Types
// =============================================================================

#[derive(Debug, Clone)]
pub struct SqlQuery {
    pub select: SelectClause,
    #[allow(dead_code)]
    pub from_alias: Option<String>,
    pub where_clause: Option<Expression>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum SelectClause {
    All,
    Columns(Vec<SelectColumn>),
    Aggregate(AggregateFunction),
}

#[derive(Debug, Clone)]
pub struct SelectColumn {
    pub name: String,
    #[allow(dead_code)]
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub enum AggregateFunction {
    Count,
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
}

#[derive(Debug, Clone)]
pub enum Expression {
    Comparison {
        left: Box<Expression>,
        op: ComparisonOp,
        right: Box<Expression>,
    },
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Column(String),
    Literal(Literal),
    Cast {
        expr: Box<Expression>,
        to_type: CastType,
    },
}

#[derive(Debug, Clone)]
pub enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
}

#[derive(Debug, Clone)]
pub enum Literal {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone)]
pub enum CastType {
    Int,
    Float,
    String,
    Bool,
}

// =============================================================================
// Request Parsing
// =============================================================================

fn parse_select_request(body: &[u8]) -> S3Result<SelectRequest> {
    let body_str = std::str::from_utf8(body)
        .map_err(|_| S3Error::new(S3ErrorCode::MalformedXML, "Invalid UTF-8 in request"))?;

    // Extract Expression
    let expression = extract_xml_value(body_str, "Expression")
        .ok_or_else(|| S3Error::new(S3ErrorCode::MalformedXML, "Missing Expression element"))?;

    // Parse InputSerialization
    let input_serialization = parse_input_serialization(body_str)?;

    // Parse OutputSerialization
    let output_serialization = parse_output_serialization(body_str)?;

    Ok(SelectRequest {
        expression,
        input_serialization,
        output_serialization,
    })
}

fn parse_input_serialization(xml: &str) -> S3Result<InputSerialization> {
    let input_section = extract_xml_section(xml, "InputSerialization")
        .ok_or_else(|| S3Error::new(S3ErrorCode::MalformedXML, "Missing InputSerialization"))?;

    let compression_type = if let Some(ct) = extract_xml_value(&input_section, "CompressionType") {
        match ct.to_uppercase().as_str() {
            "GZIP" => CompressionType::Gzip,
            "BZIP2" => CompressionType::Bzip2,
            _ => CompressionType::None,
        }
    } else {
        CompressionType::None
    };

    let format = if let Some(csv_section) = extract_xml_section(&input_section, "CSV") {
        let file_header_info = match extract_xml_value(&csv_section, "FileHeaderInfo")
            .unwrap_or_default()
            .to_uppercase()
            .as_str()
        {
            "USE" => FileHeaderInfo::Use,
            "IGNORE" => FileHeaderInfo::Ignore,
            _ => FileHeaderInfo::None,
        };

        let field_delimiter = extract_xml_value(&csv_section, "FieldDelimiter")
            .and_then(|s| s.chars().next())
            .unwrap_or(',');

        let record_delimiter = extract_xml_value(&csv_section, "RecordDelimiter")
            .and_then(|s| s.chars().next())
            .unwrap_or('\n');

        let quote_character = extract_xml_value(&csv_section, "QuoteCharacter")
            .and_then(|s| s.chars().next())
            .unwrap_or('"');

        InputFormat::Csv(CsvInput {
            file_header_info,
            field_delimiter,
            record_delimiter,
            quote_character,
        })
    } else if let Some(json_section) = extract_xml_section(&input_section, "JSON") {
        let json_type = match extract_xml_value(&json_section, "Type")
            .unwrap_or_default()
            .to_uppercase()
            .as_str()
        {
            "LINES" => JsonType::Lines,
            _ => JsonType::Document,
        };

        InputFormat::Json(JsonInput { json_type })
    } else {
        return Err(S3Error::new(
            S3ErrorCode::MalformedXML,
            "InputSerialization must specify CSV or JSON",
        ));
    };

    Ok(InputSerialization {
        format,
        compression_type,
    })
}

fn parse_output_serialization(xml: &str) -> S3Result<OutputSerialization> {
    let output_section = extract_xml_section(xml, "OutputSerialization")
        .ok_or_else(|| S3Error::new(S3ErrorCode::MalformedXML, "Missing OutputSerialization"))?;

    let format = if let Some(csv_section) = extract_xml_section(&output_section, "CSV") {
        let field_delimiter = extract_xml_value(&csv_section, "FieldDelimiter")
            .and_then(|s| s.chars().next())
            .unwrap_or(',');

        let record_delimiter = extract_xml_value(&csv_section, "RecordDelimiter")
            .and_then(|s| s.chars().next())
            .unwrap_or('\n');

        let quote_character = extract_xml_value(&csv_section, "QuoteCharacter")
            .and_then(|s| s.chars().next())
            .unwrap_or('"');

        OutputFormat::Csv(CsvOutput {
            field_delimiter,
            record_delimiter,
            quote_character,
        })
    } else if let Some(json_section) = extract_xml_section(&output_section, "JSON") {
        let record_delimiter = extract_xml_value(&json_section, "RecordDelimiter")
            .and_then(|s| s.chars().next())
            .unwrap_or('\n');

        OutputFormat::Json(JsonOutput { record_delimiter })
    } else {
        return Err(S3Error::new(
            S3ErrorCode::MalformedXML,
            "OutputSerialization must specify CSV or JSON",
        ));
    };

    Ok(OutputSerialization { format })
}

fn extract_xml_value(content: &str, tag: &str) -> Option<String> {
    let open_tag = format!("<{}>", tag);
    let close_tag = format!("</{}>", tag);

    if let Some(start) = content.find(&open_tag) {
        let after_open = &content[start + open_tag.len()..];
        if let Some(end) = after_open.find(&close_tag) {
            let value = &after_open[..end];
            // Decode common XML/HTML entities
            return Some(decode_xml_entities(value));
        }
    }
    None
}

/// Decode common XML entities in a string
fn decode_xml_entities(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

fn extract_xml_section(content: &str, tag: &str) -> Option<String> {
    let open_tag = format!("<{}", tag);
    let close_tag = format!("</{}>", tag);

    if let Some(start) = content.find(&open_tag) {
        // Find the end of opening tag
        let after_open = &content[start..];
        if let Some(tag_end) = after_open.find('>') {
            let after_tag = &after_open[tag_end + 1..];
            if let Some(end) = after_tag.find(&close_tag) {
                return Some(after_tag[..end].to_string());
            }
        }
    }
    None
}

// =============================================================================
// SQL Parser
// =============================================================================

fn parse_sql(sql: &str) -> S3Result<SqlQuery> {
    let sql = sql.trim();

    // Check for SELECT
    if !sql.to_uppercase().starts_with("SELECT") {
        return Err(S3Error::new(
            S3ErrorCode::InvalidArgument,
            "SQL query must start with SELECT",
        ));
    }

    let upper = sql.to_uppercase();

    // Find FROM position
    let from_pos = upper.find(" FROM ")
        .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidArgument, "Missing FROM clause"))?;

    // Parse SELECT clause (between SELECT and FROM)
    let select_part = sql[6..from_pos].trim();
    let select = parse_select_clause(select_part)?;

    // Parse FROM clause and get remaining
    let after_from = &sql[from_pos + 6..];
    let upper_after_from = after_from.to_uppercase();

    // Find WHERE, LIMIT positions
    let where_pos = upper_after_from.find(" WHERE ");
    let limit_pos = upper_after_from.find(" LIMIT ");

    // Extract table/alias
    let table_end = where_pos
        .or(limit_pos)
        .unwrap_or(after_from.len());
    let table_part = after_from[..table_end].trim();

    // Parse table alias (e.g., "s3object s" -> alias = "s")
    let from_alias = if table_part.to_lowercase().starts_with("s3object") {
        let parts: Vec<&str> = table_part.split_whitespace().collect();
        if parts.len() > 1 {
            Some(parts[1].to_string())
        } else {
            None
        }
    } else {
        None
    };

    // Parse WHERE clause if present
    let where_clause = if let Some(wp) = where_pos {
        let where_start = wp + 7;
        let where_end = if let Some(lp) = limit_pos {
            if lp > wp { lp } else { after_from.len() }
        } else {
            after_from.len()
        };
        let where_str = after_from[where_start..where_end].trim();
        Some(parse_expression(where_str, &from_alias)?)
    } else {
        None
    };

    // Parse LIMIT clause if present
    let limit = if let Some(lp) = limit_pos {
        let limit_str = after_from[lp + 7..].trim();
        Some(limit_str.parse::<usize>().map_err(|_| {
            S3Error::new(S3ErrorCode::InvalidArgument, "Invalid LIMIT value")
        })?)
    } else {
        None
    };

    Ok(SqlQuery {
        select,
        from_alias,
        where_clause,
        limit,
    })
}

fn parse_select_clause(select_part: &str) -> S3Result<SelectClause> {
    let select_part = select_part.trim();
    let upper = select_part.to_uppercase();

    // Check for aggregates
    if upper.starts_with("COUNT(") {
        return Ok(SelectClause::Aggregate(AggregateFunction::Count));
    }

    if upper.starts_with("SUM(") {
        let col = extract_function_arg(select_part)?;
        return Ok(SelectClause::Aggregate(AggregateFunction::Sum(col)));
    }

    if upper.starts_with("AVG(") {
        let col = extract_function_arg(select_part)?;
        return Ok(SelectClause::Aggregate(AggregateFunction::Avg(col)));
    }

    if upper.starts_with("MIN(") {
        let col = extract_function_arg(select_part)?;
        return Ok(SelectClause::Aggregate(AggregateFunction::Min(col)));
    }

    if upper.starts_with("MAX(") {
        let col = extract_function_arg(select_part)?;
        return Ok(SelectClause::Aggregate(AggregateFunction::Max(col)));
    }

    // Check for SELECT *
    if select_part == "*" {
        return Ok(SelectClause::All);
    }

    // Parse column list
    let columns: Vec<SelectColumn> = select_part
        .split(',')
        .map(|c| {
            let c = c.trim();
            // Handle alias.column format
            let name = if c.contains('.') {
                c.split('.').last().unwrap_or(c).to_string()
            } else {
                c.to_string()
            };
            SelectColumn { name, alias: None }
        })
        .collect();

    Ok(SelectClause::Columns(columns))
}

fn extract_function_arg(func_call: &str) -> S3Result<String> {
    let start = func_call.find('(').ok_or_else(|| {
        S3Error::new(S3ErrorCode::InvalidArgument, "Invalid function syntax")
    })?;
    let end = func_call.rfind(')').ok_or_else(|| {
        S3Error::new(S3ErrorCode::InvalidArgument, "Invalid function syntax")
    })?;

    let arg = func_call[start + 1..end].trim();

    // Handle CAST inside aggregate, e.g., SUM(CAST(age AS INT))
    let upper = arg.to_uppercase();
    if upper.starts_with("CAST(") {
        // Extract the column name from CAST(column AS TYPE)
        let cast_start = arg.find('(').unwrap() + 1;
        let as_pos = upper.find(" AS ").ok_or_else(|| {
            S3Error::new(S3ErrorCode::InvalidArgument, "Invalid CAST syntax")
        })?;
        return Ok(arg[cast_start..as_pos].trim().to_string());
    }

    Ok(arg.to_string())
}

fn parse_expression(expr: &str, alias: &Option<String>) -> S3Result<Expression> {
    let expr = expr.trim();
    let upper = expr.to_uppercase();

    // Handle AND/OR (split on lowest precedence first)
    if let Some(pos) = find_logical_op(&upper, " AND ") {
        let left = parse_expression(&expr[..pos], alias)?;
        let right = parse_expression(&expr[pos + 5..], alias)?;
        return Ok(Expression::And(Box::new(left), Box::new(right)));
    }

    if let Some(pos) = find_logical_op(&upper, " OR ") {
        let left = parse_expression(&expr[..pos], alias)?;
        let right = parse_expression(&expr[pos + 4..], alias)?;
        return Ok(Expression::Or(Box::new(left), Box::new(right)));
    }

    // Handle comparisons (find operators outside of parentheses)
    for (op_str, op) in [
        (">=", ComparisonOp::Ge),
        ("<=", ComparisonOp::Le),
        ("<>", ComparisonOp::Ne),
        ("!=", ComparisonOp::Ne),
        ("=", ComparisonOp::Eq),
        (">", ComparisonOp::Gt),
        ("<", ComparisonOp::Lt),
    ] {
        if let Some(pos) = find_op_outside_parens(expr, op_str) {
            let left = parse_expression(&expr[..pos], alias)?;
            let right = parse_expression(&expr[pos + op_str.len()..], alias)?;
            return Ok(Expression::Comparison {
                left: Box::new(left),
                op,
                right: Box::new(right),
            });
        }
    }

    // Handle LIKE
    if let Some(pos) = upper.find(" LIKE ") {
        let left = parse_expression(&expr[..pos], alias)?;
        let right = parse_expression(&expr[pos + 6..], alias)?;
        return Ok(Expression::Comparison {
            left: Box::new(left),
            op: ComparisonOp::Like,
            right: Box::new(right),
        });
    }

    // Handle CAST
    if upper.starts_with("CAST(") {
        let inner = &expr[5..expr.len() - 1]; // Remove CAST( and )
        let as_pos = inner.to_uppercase().find(" AS ").ok_or_else(|| {
            S3Error::new(S3ErrorCode::InvalidArgument, "Invalid CAST syntax")
        })?;
        let inner_expr = parse_expression(&inner[..as_pos], alias)?;
        let type_str = inner[as_pos + 4..].trim().to_uppercase();
        let to_type = match type_str.as_str() {
            "INT" | "INTEGER" => CastType::Int,
            "FLOAT" | "DOUBLE" | "DECIMAL" => CastType::Float,
            "STRING" | "VARCHAR" => CastType::String,
            "BOOL" | "BOOLEAN" => CastType::Bool,
            _ => {
                return Err(S3Error::new(
                    S3ErrorCode::InvalidArgument,
                    format!("Unknown type in CAST: {}", type_str),
                ))
            }
        };
        return Ok(Expression::Cast {
            expr: Box::new(inner_expr),
            to_type,
        });
    }

    // Handle string literals
    if (expr.starts_with('\'') && expr.ends_with('\''))
        || (expr.starts_with('"') && expr.ends_with('"'))
    {
        return Ok(Expression::Literal(Literal::String(
            expr[1..expr.len() - 1].to_string(),
        )));
    }

    // Handle numeric literals
    if let Ok(n) = expr.parse::<f64>() {
        return Ok(Expression::Literal(Literal::Number(n)));
    }

    // Handle boolean literals
    if upper == "TRUE" {
        return Ok(Expression::Literal(Literal::Bool(true)));
    }
    if upper == "FALSE" {
        return Ok(Expression::Literal(Literal::Bool(false)));
    }
    if upper == "NULL" {
        return Ok(Expression::Literal(Literal::Null));
    }

    // Handle column reference (possibly with alias prefix)
    let col_name = if let Some(ref a) = alias {
        let prefix = format!("{}.", a);
        if expr.to_lowercase().starts_with(&prefix.to_lowercase()) {
            expr[prefix.len()..].to_string()
        } else {
            expr.to_string()
        }
    } else {
        expr.to_string()
    };

    Ok(Expression::Column(col_name))
}

fn find_logical_op(upper: &str, op: &str) -> Option<usize> {
    // Find operator outside of parentheses and quotes
    let mut paren_depth = 0;
    let mut in_string = false;
    let bytes = upper.as_bytes();
    let op_bytes = op.as_bytes();

    for i in 0..bytes.len() {
        if bytes[i] == b'\'' || bytes[i] == b'"' {
            in_string = !in_string;
        } else if !in_string {
            if bytes[i] == b'(' {
                paren_depth += 1;
            } else if bytes[i] == b')' {
                paren_depth -= 1;
            } else if paren_depth == 0 && i + op_bytes.len() <= bytes.len() {
                if &bytes[i..i + op_bytes.len()] == op_bytes {
                    return Some(i);
                }
            }
        }
    }
    None
}

/// Find comparison operator outside of parentheses and quotes
fn find_op_outside_parens(expr: &str, op: &str) -> Option<usize> {
    let mut paren_depth = 0;
    let mut in_string = false;
    let bytes = expr.as_bytes();
    let op_bytes = op.as_bytes();

    for i in 0..bytes.len() {
        if bytes[i] == b'\'' || bytes[i] == b'"' {
            in_string = !in_string;
        } else if !in_string {
            if bytes[i] == b'(' {
                paren_depth += 1;
            } else if bytes[i] == b')' {
                paren_depth -= 1;
            } else if paren_depth == 0 && i + op_bytes.len() <= bytes.len() {
                if &bytes[i..i + op_bytes.len()] == op_bytes {
                    return Some(i);
                }
            }
        }
    }
    None
}

// =============================================================================
// Data Processing
// =============================================================================

type Row = HashMap<String, String>;

fn parse_csv_data(data: &str, config: &CsvInput) -> Vec<Row> {
    let mut rows = Vec::new();
    let lines: Vec<&str> = data.lines().collect();

    if lines.is_empty() {
        return rows;
    }

    // Parse header or generate column names
    let (headers, data_start) = match config.file_header_info {
        FileHeaderInfo::Use => {
            let header_line = lines[0];
            let headers: Vec<String> = header_line
                .split(config.field_delimiter)
                .map(|s| s.trim().to_string())
                .collect();
            (headers, 1)
        }
        FileHeaderInfo::Ignore => {
            // Skip header but use positional names
            let field_count = lines[0].split(config.field_delimiter).count();
            let headers: Vec<String> = (1..=field_count).map(|i| format!("_{}", i)).collect();
            (headers, 1)
        }
        FileHeaderInfo::None => {
            // No header - use positional names
            if lines.is_empty() {
                return rows;
            }
            let field_count = lines[0].split(config.field_delimiter).count();
            let headers: Vec<String> = (1..=field_count).map(|i| format!("_{}", i)).collect();
            (headers, 0)
        }
    };

    // Parse data rows
    for line in &lines[data_start..] {
        if line.trim().is_empty() {
            continue;
        }
        let values: Vec<&str> = line.split(config.field_delimiter).collect();
        let mut row = HashMap::new();
        for (i, header) in headers.iter().enumerate() {
            let value = values.get(i).map(|s| s.trim()).unwrap_or("");
            row.insert(header.clone(), value.to_string());
        }
        rows.push(row);
    }

    rows
}

fn parse_json_data(data: &str, config: &JsonInput) -> S3Result<Vec<Row>> {
    let mut rows = Vec::new();

    match config.json_type {
        JsonType::Document => {
            // Single JSON object
            let value: JsonValue = serde_json::from_str(data).map_err(|e| {
                S3Error::new(
                    S3ErrorCode::InvalidArgument,
                    format!("Invalid JSON: {}", e),
                )
            })?;

            if let JsonValue::Object(obj) = value {
                rows.push(json_object_to_row(&obj));
            } else if let JsonValue::Array(arr) = value {
                for item in arr {
                    if let JsonValue::Object(obj) = item {
                        rows.push(json_object_to_row(&obj));
                    }
                }
            }
        }
        JsonType::Lines => {
            // JSON Lines format
            for line in data.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let value: JsonValue = serde_json::from_str(line).map_err(|e| {
                    S3Error::new(
                        S3ErrorCode::InvalidArgument,
                        format!("Invalid JSON line: {}", e),
                    )
                })?;
                if let JsonValue::Object(obj) = value {
                    rows.push(json_object_to_row(&obj));
                }
            }
        }
    }

    Ok(rows)
}

fn json_object_to_row(obj: &Map<String, JsonValue>) -> Row {
    let mut row = HashMap::new();
    for (key, value) in obj {
        let str_value = match value {
            JsonValue::String(s) => s.clone(),
            JsonValue::Number(n) => n.to_string(),
            JsonValue::Bool(b) => b.to_string(),
            JsonValue::Null => "null".to_string(),
            _ => value.to_string(),
        };
        row.insert(key.clone(), str_value);
    }
    row
}

fn execute_query(rows: Vec<Row>, query: &SqlQuery, headers: &[String]) -> S3Result<QueryResult> {
    // Filter rows with WHERE clause
    let filtered: Vec<Row> = rows
        .into_iter()
        .filter(|row| {
            if let Some(ref where_expr) = query.where_clause {
                evaluate_expression(where_expr, row)
                    .map(|v| v.as_bool())
                    .unwrap_or(false)
            } else {
                true
            }
        })
        .collect();

    // Apply LIMIT
    let limited: Vec<Row> = if let Some(limit) = query.limit {
        filtered.into_iter().take(limit).collect()
    } else {
        filtered
    };

    // Apply SELECT clause
    match &query.select {
        SelectClause::All => Ok(QueryResult::Rows {
            rows: limited,
            columns: headers.to_vec(),
        }),
        SelectClause::Columns(cols) => {
            let column_names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
            let projected: Vec<Row> = limited
                .into_iter()
                .map(|row| {
                    let mut new_row = HashMap::new();
                    for col in cols {
                        if let Some(val) = row.get(&col.name) {
                            new_row.insert(col.name.clone(), val.clone());
                        }
                    }
                    new_row
                })
                .collect();
            Ok(QueryResult::Rows {
                rows: projected,
                columns: column_names,
            })
        }
        SelectClause::Aggregate(agg) => {
            let result = match agg {
                AggregateFunction::Count => limited.len() as f64,
                AggregateFunction::Sum(col) => {
                    limited
                        .iter()
                        .filter_map(|row| row.get(col))
                        .filter_map(|v| v.parse::<f64>().ok())
                        .sum()
                }
                AggregateFunction::Avg(col) => {
                    let values: Vec<f64> = limited
                        .iter()
                        .filter_map(|row| row.get(col))
                        .filter_map(|v| v.parse::<f64>().ok())
                        .collect();
                    if values.is_empty() {
                        0.0
                    } else {
                        values.iter().sum::<f64>() / values.len() as f64
                    }
                }
                AggregateFunction::Min(col) => {
                    limited
                        .iter()
                        .filter_map(|row| row.get(col))
                        .filter_map(|v| v.parse::<f64>().ok())
                        .fold(f64::INFINITY, f64::min)
                }
                AggregateFunction::Max(col) => {
                    limited
                        .iter()
                        .filter_map(|row| row.get(col))
                        .filter_map(|v| v.parse::<f64>().ok())
                        .fold(f64::NEG_INFINITY, f64::max)
                }
            };
            Ok(QueryResult::Aggregate(result))
        }
    }
}

enum QueryResult {
    Rows { rows: Vec<Row>, columns: Vec<String> },
    Aggregate(f64),
}

#[derive(Debug, Clone)]
enum Value {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
}

impl Value {
    fn as_bool(&self) -> bool {
        match self {
            Value::Bool(b) => *b,
            Value::Number(n) => *n != 0.0,
            Value::String(s) => !s.is_empty(),
            Value::Null => false,
        }
    }

    fn as_number(&self) -> Option<f64> {
        match self {
            Value::Number(n) => Some(*n),
            Value::String(s) => s.parse().ok(),
            Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
            Value::Null => None,
        }
    }

    fn as_string(&self) -> String {
        match self {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => "null".to_string(),
        }
    }
}

fn evaluate_expression(expr: &Expression, row: &Row) -> S3Result<Value> {
    match expr {
        Expression::Column(name) => {
            Ok(row
                .get(name)
                .map(|s| Value::String(s.clone()))
                .unwrap_or(Value::Null))
        }
        Expression::Literal(lit) => match lit {
            Literal::String(s) => Ok(Value::String(s.clone())),
            Literal::Number(n) => Ok(Value::Number(*n)),
            Literal::Bool(b) => Ok(Value::Bool(*b)),
            Literal::Null => Ok(Value::Null),
        },
        Expression::Cast { expr, to_type } => {
            let val = evaluate_expression(expr, row)?;
            match to_type {
                CastType::Int => {
                    let n = val.as_number().unwrap_or(0.0) as i64;
                    Ok(Value::Number(n as f64))
                }
                CastType::Float => Ok(Value::Number(val.as_number().unwrap_or(0.0))),
                CastType::String => Ok(Value::String(val.as_string())),
                CastType::Bool => Ok(Value::Bool(val.as_bool())),
            }
        }
        Expression::Comparison { left, op, right } => {
            let l = evaluate_expression(left, row)?;
            let r = evaluate_expression(right, row)?;

            let result = match op {
                ComparisonOp::Eq => match (&l, &r) {
                    (Value::Number(a), Value::Number(b)) => (a - b).abs() < f64::EPSILON,
                    _ => l.as_string() == r.as_string(),
                },
                ComparisonOp::Ne => match (&l, &r) {
                    (Value::Number(a), Value::Number(b)) => (a - b).abs() >= f64::EPSILON,
                    _ => l.as_string() != r.as_string(),
                },
                ComparisonOp::Lt => {
                    match (l.as_number(), r.as_number()) {
                        (Some(a), Some(b)) => a < b,
                        _ => l.as_string() < r.as_string(),
                    }
                }
                ComparisonOp::Le => {
                    match (l.as_number(), r.as_number()) {
                        (Some(a), Some(b)) => a <= b,
                        _ => l.as_string() <= r.as_string(),
                    }
                }
                ComparisonOp::Gt => {
                    match (l.as_number(), r.as_number()) {
                        (Some(a), Some(b)) => a > b,
                        _ => l.as_string() > r.as_string(),
                    }
                }
                ComparisonOp::Ge => {
                    match (l.as_number(), r.as_number()) {
                        (Some(a), Some(b)) => a >= b,
                        _ => l.as_string() >= r.as_string(),
                    }
                }
                ComparisonOp::Like => {
                    let pattern = r.as_string();
                    let text = l.as_string();
                    like_match(&text, &pattern)
                }
            };
            Ok(Value::Bool(result))
        }
        Expression::And(l, r) => {
            let lv = evaluate_expression(l, row)?.as_bool();
            let rv = evaluate_expression(r, row)?.as_bool();
            Ok(Value::Bool(lv && rv))
        }
        Expression::Or(l, r) => {
            let lv = evaluate_expression(l, row)?.as_bool();
            let rv = evaluate_expression(r, row)?.as_bool();
            Ok(Value::Bool(lv || rv))
        }
    }
}

fn like_match(text: &str, pattern: &str) -> bool {
    // Simple LIKE implementation: % = any chars, _ = single char
    // Simple matching without full regex
    if pattern.contains('%') || pattern.contains('_') {
        let parts: Vec<&str> = pattern.split('%').collect();
        if parts.len() == 1 {
            // No %, just _ wildcards
            if text.len() != pattern.len() {
                return false;
            }
            for (tc, pc) in text.chars().zip(pattern.chars()) {
                if pc != '_' && tc != pc {
                    return false;
                }
            }
            true
        } else {
            // Has % wildcards
            let mut pos = 0;
            for (i, part) in parts.iter().enumerate() {
                if part.is_empty() {
                    continue;
                }
                if i == 0 {
                    // Must start with this part
                    if !text.starts_with(part) {
                        return false;
                    }
                    pos = part.len();
                } else if i == parts.len() - 1 {
                    // Must end with this part
                    if !text.ends_with(part) {
                        return false;
                    }
                } else {
                    // Must contain this part after current position
                    if let Some(found) = text[pos..].find(part) {
                        pos += found + part.len();
                    } else {
                        return false;
                    }
                }
            }
            true
        }
    } else {
        text == pattern
    }
}

// =============================================================================
// Output Serialization
// =============================================================================

fn serialize_csv(result: &QueryResult, config: &CsvOutput) -> String {
    let mut output = String::new();

    match result {
        QueryResult::Rows { rows, columns } => {
            for row in rows {
                let values: Vec<String> = columns
                    .iter()
                    .map(|col| row.get(col).cloned().unwrap_or_default())
                    .collect();
                output.push_str(&values.join(&config.field_delimiter.to_string()));
                output.push(config.record_delimiter);
            }
        }
        QueryResult::Aggregate(value) => {
            // Format aggregate result - remove unnecessary decimal places
            if value.fract() == 0.0 {
                output.push_str(&format!("{}", *value as i64));
            } else {
                output.push_str(&format!("{}", value));
            }
            output.push(config.record_delimiter);
        }
    }

    output
}

fn serialize_json(result: &QueryResult, config: &JsonOutput) -> String {
    let mut output = String::new();

    match result {
        QueryResult::Rows { rows, columns } => {
            for row in rows {
                let mut obj = serde_json::Map::new();
                for col in columns {
                    if let Some(val) = row.get(col) {
                        // Try to parse as number, otherwise use string
                        if let Ok(n) = val.parse::<i64>() {
                            obj.insert(col.clone(), serde_json::Value::Number(n.into()));
                        } else if let Ok(n) = val.parse::<f64>() {
                            if let Some(num) = serde_json::Number::from_f64(n) {
                                obj.insert(col.clone(), serde_json::Value::Number(num));
                            } else {
                                obj.insert(col.clone(), serde_json::Value::String(val.clone()));
                            }
                        } else {
                            obj.insert(col.clone(), serde_json::Value::String(val.clone()));
                        }
                    }
                }
                output.push_str(&serde_json::to_string(&obj).unwrap_or_default());
                output.push(config.record_delimiter);
            }
        }
        QueryResult::Aggregate(value) => {
            let obj = serde_json::json!({ "_1": value });
            output.push_str(&serde_json::to_string(&obj).unwrap_or_default());
            output.push(config.record_delimiter);
        }
    }

    output
}

// =============================================================================
// EventStream Response
// =============================================================================

/// Build an EventStream response for SelectObjectContent
///
/// The EventStream format consists of binary messages with:
/// - Prelude (8 bytes): total length (4), headers length (4)
/// - Headers: key-value pairs with type information
/// - Payload: the actual data
/// - Message CRC (4 bytes)
fn build_event_stream_response(data: &str, bytes_scanned: u64, bytes_returned: u64) -> Bytes {
    let mut output = BytesMut::new();

    // Records event with data
    if !data.is_empty() {
        let records_event = build_event_message(":event-type", "Records", data.as_bytes());
        output.extend_from_slice(&records_event);
    }

    // Stats event
    let stats_xml = format!(
        "<Stats><BytesScanned>{}</BytesScanned><BytesProcessed>{}</BytesProcessed><BytesReturned>{}</BytesReturned></Stats>",
        bytes_scanned, bytes_scanned, bytes_returned
    );
    let stats_event = build_event_message(":event-type", "Stats", stats_xml.as_bytes());
    output.extend_from_slice(&stats_event);

    // End event
    let end_event = build_event_message(":event-type", "End", &[]);
    output.extend_from_slice(&end_event);

    output.freeze()
}

fn build_event_message(header_name: &str, header_value: &str, payload: &[u8]) -> Vec<u8> {
    // Build headers first to know their size
    let mut headers = Vec::new();

    // :message-type header (always "event")
    add_header(&mut headers, ":message-type", "event");

    // Event type header
    add_header(&mut headers, header_name, header_value);

    // :content-type header for Records
    if header_value == "Records" {
        add_header(&mut headers, ":content-type", "application/octet-stream");
    } else if header_value == "Stats" {
        add_header(&mut headers, ":content-type", "text/xml");
    }

    let headers_len = headers.len() as u32;
    // Total length: prelude(8) + prelude_crc(4) + headers + payload + message_crc(4)
    let total_len = 8 + 4 + headers_len + payload.len() as u32 + 4;

    let mut message = Vec::with_capacity(total_len as usize);

    // Prelude: total_length (4 bytes) + headers_length (4 bytes)
    message.extend_from_slice(&total_len.to_be_bytes());
    message.extend_from_slice(&headers_len.to_be_bytes());

    // Prelude CRC (4 bytes) - CRC of the 8-byte prelude
    let prelude_crc = crc32_checksum(&message[..8]);
    message.extend_from_slice(&prelude_crc.to_be_bytes());

    // Headers
    message.extend_from_slice(&headers);

    // Payload
    message.extend_from_slice(payload);

    // Message CRC (4 bytes) - CRC of everything up to this point
    let message_crc = crc32_checksum(&message);
    message.extend_from_slice(&message_crc.to_be_bytes());

    message
}

fn add_header(headers: &mut Vec<u8>, name: &str, value: &str) {
    // Header name length (1 byte)
    headers.push(name.len() as u8);
    // Header name
    headers.extend_from_slice(name.as_bytes());
    // Header value type (7 = string)
    headers.push(7);
    // Header value length (2 bytes, big endian)
    headers.extend_from_slice(&(value.len() as u16).to_be_bytes());
    // Header value
    headers.extend_from_slice(value.as_bytes());
}

/// CRC32 checksum using the crc32fast crate
/// Note: AWS EventStream uses CRC-32 (ISO 3309), not CRC-32C (Castagnoli)
fn crc32_checksum(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

// =============================================================================
// Main Handler
// =============================================================================

/// Handle SelectObjectContent request
pub async fn select_object_content(
    storage: &StorageEngine,
    bucket: &str,
    key: &str,
    body: Bytes,
) -> S3Result<Response<Full<Bytes>>> {
    // Check bucket exists
    if !storage.bucket_exists(bucket).await {
        return Err(S3Error::no_such_bucket(bucket));
    }

    // Check object exists
    if !storage.object_exists(bucket, key).await {
        return Err(S3Error::no_such_key(key));
    }

    // Parse request
    let request = parse_select_request(&body)?;

    // Check for unsupported compression
    if !matches!(request.input_serialization.compression_type, CompressionType::None) {
        return Err(S3Error::new(
            S3ErrorCode::InvalidArgument,
            "Compression is not currently supported",
        ));
    }

    // Get object data
    let (_obj, obj_data) = storage.get_object(bucket, key).await?;
    let data = String::from_utf8(obj_data.to_vec()).map_err(|_| {
        S3Error::new(S3ErrorCode::InvalidArgument, "Object data is not valid UTF-8")
    })?;

    let bytes_scanned = data.len() as u64;

    // Parse input data
    let (rows, headers) = match &request.input_serialization.format {
        InputFormat::Csv(csv_config) => {
            let rows = parse_csv_data(&data, csv_config);
            let headers = if rows.is_empty() {
                Vec::new()
            } else {
                rows[0].keys().cloned().collect()
            };
            (rows, headers)
        }
        InputFormat::Json(json_config) => {
            let rows = parse_json_data(&data, json_config)?;
            let headers = if rows.is_empty() {
                Vec::new()
            } else {
                rows[0].keys().cloned().collect()
            };
            (rows, headers)
        }
    };

    // Parse and execute SQL query
    let query = parse_sql(&request.expression)?;
    let result = execute_query(rows, &query, &headers)?;

    // Serialize output
    let output_data = match &request.output_serialization.format {
        OutputFormat::Csv(csv_config) => serialize_csv(&result, csv_config),
        OutputFormat::Json(json_config) => serialize_json(&result, json_config),
    };

    let bytes_returned = output_data.len() as u64;

    // Build EventStream response
    let event_stream = build_event_stream_response(&output_data, bytes_scanned, bytes_returned);

    let response = Response::builder()
        .status(200)
        .header("Content-Type", "application/octet-stream")
        .header("x-amz-request-id", uuid::Uuid::new_v4().to_string())
        .body(Full::new(event_stream))
        .map_err(|e| S3Error::new(S3ErrorCode::InternalError, e.to_string()))?;

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use tempfile::TempDir;

    async fn create_test_storage() -> (StorageEngine, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = Config::new().with_data_dir(temp_dir.path());
        let storage = StorageEngine::new(config).await.unwrap();
        (storage, temp_dir)
    }

    // ===================
    // SQL Parser Tests
    // ===================

    #[test]
    fn test_parse_select_all() {
        let query = parse_sql("SELECT * FROM s3object").unwrap();
        assert!(matches!(query.select, SelectClause::All));
        assert!(query.where_clause.is_none());
        assert!(query.limit.is_none());
    }

    #[test]
    fn test_parse_select_columns() {
        let query = parse_sql("SELECT name, age FROM s3object").unwrap();
        if let SelectClause::Columns(cols) = &query.select {
            assert_eq!(cols.len(), 2);
            assert_eq!(cols[0].name, "name");
            assert_eq!(cols[1].name, "age");
        } else {
            panic!("Expected Columns clause");
        }
    }

    #[test]
    fn test_parse_select_with_alias() {
        let query = parse_sql("SELECT s.name, s.city FROM s3object s").unwrap();
        assert_eq!(query.from_alias, Some("s".to_string()));
        if let SelectClause::Columns(cols) = &query.select {
            assert_eq!(cols[0].name, "name");
            assert_eq!(cols[1].name, "city");
        } else {
            panic!("Expected Columns clause");
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let query = parse_sql("SELECT * FROM s3object WHERE age > 25").unwrap();
        assert!(query.where_clause.is_some());
    }

    #[test]
    fn test_parse_select_with_limit() {
        let query = parse_sql("SELECT * FROM s3object LIMIT 10").unwrap();
        assert_eq!(query.limit, Some(10));
    }

    #[test]
    fn test_parse_count() {
        let query = parse_sql("SELECT COUNT(*) FROM s3object").unwrap();
        assert!(matches!(
            query.select,
            SelectClause::Aggregate(AggregateFunction::Count)
        ));
    }

    #[test]
    fn test_parse_sum() {
        let query = parse_sql("SELECT SUM(age) FROM s3object").unwrap();
        if let SelectClause::Aggregate(AggregateFunction::Sum(col)) = &query.select {
            assert_eq!(col, "age");
        } else {
            panic!("Expected Sum aggregate");
        }
    }

    #[test]
    fn test_parse_sum_with_cast() {
        let query = parse_sql("SELECT SUM(CAST(age AS INT)) FROM s3object").unwrap();
        if let SelectClause::Aggregate(AggregateFunction::Sum(col)) = &query.select {
            assert_eq!(col, "age");
        } else {
            panic!("Expected Sum aggregate");
        }
    }

    #[test]
    fn test_parse_invalid_sql() {
        let result = parse_sql("INVALID SQL");
        assert!(result.is_err());
    }

    // ===================
    // CSV Parser Tests
    // ===================

    #[test]
    fn test_parse_csv_with_header() {
        let data = "name,age,city\nAlice,30,NYC\nBob,25,LA\n";
        let config = CsvInput {
            file_header_info: FileHeaderInfo::Use,
            field_delimiter: ',',
            record_delimiter: '\n',
            quote_character: '"',
        };
        let rows = parse_csv_data(data, &config);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("name").unwrap(), "Alice");
        assert_eq!(rows[0].get("age").unwrap(), "30");
        assert_eq!(rows[1].get("name").unwrap(), "Bob");
    }

    #[test]
    fn test_parse_csv_no_header() {
        let data = "Alice,30,NYC\nBob,25,LA\n";
        let config = CsvInput {
            file_header_info: FileHeaderInfo::None,
            field_delimiter: ',',
            record_delimiter: '\n',
            quote_character: '"',
        };
        let rows = parse_csv_data(data, &config);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("_1").unwrap(), "Alice");
        assert_eq!(rows[0].get("_2").unwrap(), "30");
    }

    // ===================
    // JSON Parser Tests
    // ===================

    #[test]
    fn test_parse_json_document() {
        let data = r#"{"name": "Alice", "age": 30}"#;
        let config = JsonInput {
            json_type: JsonType::Document,
        };
        let rows = parse_json_data(data, &config).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("name").unwrap(), "Alice");
        assert_eq!(rows[0].get("age").unwrap(), "30");
    }

    #[test]
    fn test_parse_json_lines() {
        let data = r#"{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}"#;
        let config = JsonInput {
            json_type: JsonType::Lines,
        };
        let rows = parse_json_data(data, &config).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("name").unwrap(), "Alice");
        assert_eq!(rows[1].get("name").unwrap(), "Bob");
    }

    // ===================
    // Expression Evaluation Tests
    // ===================

    #[test]
    fn test_evaluate_comparison() {
        let mut row = HashMap::new();
        row.insert("age".to_string(), "30".to_string());

        let expr = Expression::Comparison {
            left: Box::new(Expression::Cast {
                expr: Box::new(Expression::Column("age".to_string())),
                to_type: CastType::Int,
            }),
            op: ComparisonOp::Gt,
            right: Box::new(Expression::Literal(Literal::Number(25.0))),
        };

        let result = evaluate_expression(&expr, &row).unwrap();
        assert!(result.as_bool());
    }

    #[test]
    fn test_evaluate_string_equals() {
        let mut row = HashMap::new();
        row.insert("city".to_string(), "NYC".to_string());

        let expr = Expression::Comparison {
            left: Box::new(Expression::Column("city".to_string())),
            op: ComparisonOp::Eq,
            right: Box::new(Expression::Literal(Literal::String("NYC".to_string()))),
        };

        let result = evaluate_expression(&expr, &row).unwrap();
        assert!(result.as_bool());
    }

    #[test]
    fn test_evaluate_and() {
        let mut row = HashMap::new();
        row.insert("age".to_string(), "30".to_string());
        row.insert("city".to_string(), "NYC".to_string());

        let expr = Expression::And(
            Box::new(Expression::Comparison {
                left: Box::new(Expression::Column("city".to_string())),
                op: ComparisonOp::Eq,
                right: Box::new(Expression::Literal(Literal::String("NYC".to_string()))),
            }),
            Box::new(Expression::Comparison {
                left: Box::new(Expression::Cast {
                    expr: Box::new(Expression::Column("age".to_string())),
                    to_type: CastType::Int,
                }),
                op: ComparisonOp::Gt,
                right: Box::new(Expression::Literal(Literal::Number(25.0))),
            }),
        );

        let result = evaluate_expression(&expr, &row).unwrap();
        assert!(result.as_bool());
    }

    // ===================
    // Query Execution Tests
    // ===================

    #[test]
    fn test_execute_select_all() {
        let mut row1 = HashMap::new();
        row1.insert("name".to_string(), "Alice".to_string());
        row1.insert("age".to_string(), "30".to_string());

        let mut row2 = HashMap::new();
        row2.insert("name".to_string(), "Bob".to_string());
        row2.insert("age".to_string(), "25".to_string());

        let rows = vec![row1, row2];
        let headers = vec!["name".to_string(), "age".to_string()];

        let query = SqlQuery {
            select: SelectClause::All,
            from_alias: None,
            where_clause: None,
            limit: None,
        };

        let result = execute_query(rows, &query, &headers).unwrap();
        if let QueryResult::Rows { rows, .. } = result {
            assert_eq!(rows.len(), 2);
        } else {
            panic!("Expected Rows result");
        }
    }

    #[test]
    fn test_execute_count() {
        let mut row1 = HashMap::new();
        row1.insert("name".to_string(), "Alice".to_string());

        let mut row2 = HashMap::new();
        row2.insert("name".to_string(), "Bob".to_string());

        let rows = vec![row1, row2];
        let headers = vec!["name".to_string()];

        let query = SqlQuery {
            select: SelectClause::Aggregate(AggregateFunction::Count),
            from_alias: None,
            where_clause: None,
            limit: None,
        };

        let result = execute_query(rows, &query, &headers).unwrap();
        if let QueryResult::Aggregate(count) = result {
            assert_eq!(count, 2.0);
        } else {
            panic!("Expected Aggregate result");
        }
    }

    #[test]
    fn test_execute_with_limit() {
        let mut row1 = HashMap::new();
        row1.insert("name".to_string(), "Alice".to_string());

        let mut row2 = HashMap::new();
        row2.insert("name".to_string(), "Bob".to_string());

        let mut row3 = HashMap::new();
        row3.insert("name".to_string(), "Charlie".to_string());

        let rows = vec![row1, row2, row3];
        let headers = vec!["name".to_string()];

        let query = SqlQuery {
            select: SelectClause::All,
            from_alias: None,
            where_clause: None,
            limit: Some(2),
        };

        let result = execute_query(rows, &query, &headers).unwrap();
        if let QueryResult::Rows { rows, .. } = result {
            assert_eq!(rows.len(), 2);
        } else {
            panic!("Expected Rows result");
        }
    }

    // ===================
    // Output Serialization Tests
    // ===================

    #[test]
    fn test_serialize_csv() {
        let mut row1 = HashMap::new();
        row1.insert("name".to_string(), "Alice".to_string());
        row1.insert("age".to_string(), "30".to_string());

        let result = QueryResult::Rows {
            rows: vec![row1],
            columns: vec!["name".to_string(), "age".to_string()],
        };

        let config = CsvOutput {
            field_delimiter: ',',
            record_delimiter: '\n',
            quote_character: '"',
        };

        let output = serialize_csv(&result, &config);
        assert!(output.contains("Alice"));
        assert!(output.contains("30"));
    }

    #[test]
    fn test_serialize_json() {
        let mut row1 = HashMap::new();
        row1.insert("name".to_string(), "Alice".to_string());
        row1.insert("age".to_string(), "30".to_string());

        let result = QueryResult::Rows {
            rows: vec![row1],
            columns: vec!["name".to_string(), "age".to_string()],
        };

        let config = JsonOutput {
            record_delimiter: '\n',
        };

        let output = serialize_json(&result, &config);
        assert!(output.contains("\"name\""));
        assert!(output.contains("Alice"));
    }

    // ===================
    // EventStream Tests
    // ===================

    #[test]
    fn test_build_event_stream() {
        let data = "Alice,30,NYC\n";
        let event_stream = build_event_stream_response(data, 100, 14);

        // Should contain Records, Stats, and End events
        assert!(!event_stream.is_empty());
        // Basic sanity check - should be multiple events
        assert!(event_stream.len() > 50);
    }

    #[test]
    fn test_crc32() {
        // Test CRC32 against known test vector
        let crc = crc32_checksum(b"123456789");
        // Standard CRC-32 (ISO 3309) test vector
        assert_eq!(crc, 0xCBF43926);

        // Test consistency
        let crc2 = crc32_checksum(b"hello");
        let crc3 = crc32_checksum(b"hello");
        assert_eq!(crc2, crc3);
    }

    // ===================
    // Integration Tests
    // ===================

    #[tokio::test]
    async fn test_select_csv_basic() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        storage
            .put_object(
                "test-bucket",
                "test.csv",
                Bytes::from("name,age,city\nAlice,30,NYC\nBob,25,LA\n"),
                Some("text/csv"),
                None,
            )
            .await
            .unwrap();

        let request_body = r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * FROM s3object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CSV>
            <FileHeaderInfo>USE</FileHeaderInfo>
        </CSV>
        <CompressionType>NONE</CompressionType>
    </InputSerialization>
    <OutputSerialization>
        <CSV></CSV>
    </OutputSerialization>
</SelectObjectContentRequest>"#;

        let result = select_object_content(
            &storage,
            "test-bucket",
            "test.csv",
            Bytes::from(request_body),
        )
        .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.status(), 200);
    }

    #[tokio::test]
    async fn test_select_bucket_not_found() {
        let (storage, _temp) = create_test_storage().await;

        let result =
            select_object_content(&storage, "nonexistent", "key", Bytes::from("body")).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::NoSuchBucket);
    }

    #[tokio::test]
    async fn test_select_key_not_found() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let result =
            select_object_content(&storage, "test-bucket", "nonexistent", Bytes::from("body"))
                .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::NoSuchKey);
    }

    #[tokio::test]
    async fn test_select_json_lines() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        storage
            .put_object(
                "test-bucket",
                "test.jsonl",
                Bytes::from(r#"{"name":"Alice","age":30}
{"name":"Bob","age":25}"#),
                Some("application/x-ndjson"),
                None,
            )
            .await
            .unwrap();

        let request_body = r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * FROM s3object</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <JSON>
            <Type>LINES</Type>
        </JSON>
        <CompressionType>NONE</CompressionType>
    </InputSerialization>
    <OutputSerialization>
        <JSON></JSON>
    </OutputSerialization>
</SelectObjectContentRequest>"#;

        let result = select_object_content(
            &storage,
            "test-bucket",
            "test.jsonl",
            Bytes::from(request_body),
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_select_with_where() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();
        storage
            .put_object(
                "test-bucket",
                "test.csv",
                Bytes::from("name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,NYC\n"),
                Some("text/csv"),
                None,
            )
            .await
            .unwrap();

        let request_body = r#"<?xml version="1.0" encoding="UTF-8"?>
<SelectObjectContentRequest>
    <Expression>SELECT * FROM s3object WHERE city = 'NYC'</Expression>
    <ExpressionType>SQL</ExpressionType>
    <InputSerialization>
        <CSV>
            <FileHeaderInfo>USE</FileHeaderInfo>
        </CSV>
        <CompressionType>NONE</CompressionType>
    </InputSerialization>
    <OutputSerialization>
        <CSV></CSV>
    </OutputSerialization>
</SelectObjectContentRequest>"#;

        let result = select_object_content(
            &storage,
            "test-bucket",
            "test.csv",
            Bytes::from(request_body),
        )
        .await;

        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_cast_in_where() {
        // Test parsing WHERE CAST(age AS INT) > 28
        let query = parse_sql("SELECT * FROM s3object WHERE CAST(age AS INT) > 28").unwrap();
        assert!(query.where_clause.is_some(), "WHERE clause should be parsed");

        // The WHERE clause should be a Comparison with CAST on the left
        if let Some(Expression::Comparison { left, op, right }) = query.where_clause {
            assert!(matches!(op, ComparisonOp::Gt), "Should be > comparison");
            assert!(matches!(*left, Expression::Cast { .. }), "Left should be CAST");
            assert!(matches!(*right, Expression::Literal(Literal::Number(_))), "Right should be number");
        } else {
            panic!("WHERE clause should be a Comparison");
        }
    }

    #[test]
    fn test_parse_expression_cast_comparison() {
        // Test that CAST expressions with comparison operators parse correctly
        let result = parse_expression("CAST(age AS INT) > 28", &None);
        assert!(result.is_ok(), "Parsing should succeed");
        let parsed = result.unwrap();
        assert!(matches!(parsed, Expression::Comparison { .. }), "Should be Comparison");
    }
}
