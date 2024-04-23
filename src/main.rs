/*
    dump
-------------------------------------------------------------------------------*/
use std::env;

use mysql::*;
use mysql::prelude::*;

use regex::Regex;

use chrono::Local;
use chrono::format::strftime::StrftimeItems;


fn main() {
    let version = "0.1.0";

    // 引数の処理
    let argv: Vec<String> = env::args().collect();
    let argc = argv.len();

    let mut db_host = "localhost";
    let mut db_user = "";
    let mut db_pass = "";
    let mut db_name = "";

    let mut i = 1;
    while i < argc {
        if argv[i] == "-h" { i += 1; db_host = &argv[i]; }
        else if argv[i] == "-u" { i += 1; db_user = &argv[i]; }
        else if argv[i] == "-p" { i += 1; db_pass = &argv[i]; }
        else if argv[i] == "-h" { i += 1; db_pass = &argv[i]; }
        else { db_name = &argv[i]; }
        i += 1;
    }

    // usage
    if db_name.is_empty() || db_user.is_empty() {
        println!("dump ver.{}", version);
        println!("Usage: dump -h <host> -u <user> -p <password> <database>");
        std::process::exit(1);
    }

    // MySQLに接続するための情報
    let mut url = format!("mysql://{}:{}@{}/{}", db_user, db_pass, db_host, db_name);   // String型
    if db_pass.is_empty() {
        url = format!("mysql://{}@{}/{}", db_user, db_host, db_name);   // String型
    }

    // MySQLに接続
    let pool = Pool::new(url.as_str()).unwrap();    // Pool::new() には &str型を指定する必要がある
    let mut conn = pool.get_conn().unwrap();

    // dump header
    print_dump_header(&mut conn, &db_name, &version);

    // テーブルの一覧を取得する
    let tables = get_table_list(&mut conn, &db_name);

    // テーブルをロックする
    let column = format!("Tables_in_{}", db_name);
    let column = column.as_str();   // シャドーイング column は &str型
    // lock_tables(&mut conn, &tables, &column);
    lock_tables(&mut conn, &tables, column);

    // テーブルの値をダンプする
    for t in tables {
        let table: String = t.get(column).unwrap(); // table: テーブル名

        // CREATE TABLE 文を出力する
        print_create_table(&mut conn, &table);

        // LOCK TABLES 文を出力する
        println!("LOCK TABLES `{}` WRITE;", table);

        // カラムのメタ情報を取得
        let sql = format!("SHOW COLUMNS FROM `{}`", table);
        let columns: Vec<Row> = conn.query(&sql).unwrap();

        // INSERT文のヘッダを出力
        print_insert_header(&columns, &table);

        // データを出力
        print_insert_data(&mut conn, &table, &columns);

        // UNLOCK TABLES 文を出力
        println!("UNLOCK TABLES;\n");
    }

}


// INSERT 文のデータを出力する
fn print_insert_data(conn: &mut mysql::PooledConn, table: &str, columns: &Vec<Row>) {
    let sql = format!("SELECT * FROM `{}`", table);
    let query_result = conn.query_iter(&sql).unwrap();

    let mut out_buffer = String::new();
    for row_result in query_result {
        if !out_buffer.is_empty() {
            println!("({}),", out_buffer);
        }

        let mut insert_data = String::new();
        let row = row_result.unwrap();
        for column in columns {
            let f: String = column.get("Field").unwrap();
            let t: String = column.get("Type").unwrap();

            let mut val: String = row.get(f.as_str()).unwrap();
            if t.to_lowercase().contains("char") || t.to_lowercase().contains("text") {
                val = format!("'{}'", val);
            }
            val = format!(",{}", val);
            insert_data.push_str(&val);
        }
        insert_data.remove(0);
        out_buffer = insert_data;
    }
    println!("({});", out_buffer);
}


// INSERT 文のヘッダを出力する
fn print_insert_header(columns: &Vec<Row>, table: &str) {
    let mut col_names = String::new();
    for column in columns {
        let f: String = column.get("Field").unwrap();
        let f = format!(",`{}`", f);
        let f = f.as_str();
        col_names.push_str(f);
    }
    col_names.remove(0);

    println!("INSERT INTO `{}` ({}) values", table, col_names);
}


// CREATE TABLE 文を出力する
fn print_create_table(conn: &mut mysql::PooledConn, table: &str) {
    let sql = format!("SHOW CREATE TABLE `{}`", table);
    let result: Vec<Row> = conn.query(&sql).unwrap();
    // Row { Table: Bytes("test"), Create Table: Bytes("CREATE T..") }

    // CREATE TABLE 文を出力する（全文出力）
    for res in &result {
        let query: String = res.get("Create Table").unwrap();
        println!("{};\n", query);
    }
}


// テーブルをロックする
fn lock_tables(conn: &mut mysql::PooledConn, tables: &Vec<Row>, column: &str) {
    let mut table_lock_query = String::new();

    for table in tables {
        let table_name: String = table.get(column).unwrap();
        let table_name = format!(",`{}` READ", table_name);
        let table_name = table_name.as_str();
        table_lock_query.push_str(table_name);
    }
    table_lock_query.push(';');
    table_lock_query.remove(0);
    table_lock_query.insert_str(0, "LOCK TABLES ");
    conn.query_drop(&table_lock_query).unwrap();     // tables lock!
}


// テーブルの一覧を取得する
fn get_table_list(conn: &mut mysql::PooledConn, db_name: &str) -> Vec<Row> {
    let sql = format!("SHOW TABLES FROM `{}`", db_name);
    let sql = sql.as_str();     // シャドーイング sql は &str型
    let tables: Vec<Row> = conn.query(sql).unwrap();

    tables
}


// print dump_header
fn print_dump_header(conn: &mut mysql::PooledConn, db_name: &str, version: &str) {
    let now = Local::now();
    let formatted_date_time = now.format_with_items(StrftimeItems::new("%Y-%m-%d %H:%M:%S"));

    println!("-- dbdump {} {}", version, formatted_date_time);
    println!("SET time_zone = '+00:00';");
    println!("SET foreign_key_checks = 0;");
    println!("SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO';");

    let sql = format!("SHOW CREATE DATABASE `{}`", db_name);
    let result: Vec<Row> = conn.query(&sql).unwrap();

    // 1行のみ取得
    if !result.is_empty() {
        let create_database_statement: String = result[0].get("Create Database").unwrap();
        // println!("{}", create_database_statement);

        // 正規表現パターンを定義
        let re = Regex::new(r"CHARACTER\s+SET\s+(\w+)").unwrap();

        // 正規表現にマッチする部分を抽出
        if let Some(captures) = re.captures(&create_database_statement) {
            if let Some(charset) = captures.get(1) {
                println!("SET NAMES {};\n", charset.as_str());
            }
        }
    }

    println!("DROP DATABASE IF EXISTS `{}`;", db_name);
    println!("CREATE DATABASE `{}`;", db_name);
    println!("USE `{}`;\n", db_name);
}

/*
    export PKG_CONFIG_PATH=/usr/lib/x86_64-linux-gnu/pkgconfig:$PKG_CONFIG_PATH
*/
