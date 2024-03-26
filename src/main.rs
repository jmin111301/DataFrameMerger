use polars::prelude::*;
use polars_lazy::prelude::*;
use polars::df;
use crate::dataframemerger::*;
use std::time::{Instant};
use std::ops::Index;
use std::cmp;
use std::thread;
use rayon::iter::*;
use::rust_fuzzy_search::fuzzy_compare;

// use fuzzy_matcher::FuzzyMatcher;
// use fuzzy_matcher::skim::SkimMatcherV2;
// use scorer::*;
// use std::collections::HashMap;
// use std::iter::zip;

pub mod dataframemerger;

// A DataFrameMerger struct. 
// - hierarchy: a vector of filters (polars expressions), which can use data from
//              the left key, atomic values, etc to filter the right dataframe
// - thresholds: a vector of thresholds. A threshold is a floating point between 0 and 1 (inclusive)
//               where two keys are rejected as a match if their similarity score is below the threshold
// - scorers: a vector of scorers (functions), which take in two strings and output a 
//            similarity score (floating point between 0 and 1, inclusive)s

fn main() {
    let df_a: DataFrame = df![
        "counts" => [1, 2, 1, 1],
        "name" => ["Jonathan", "Jack", "Amanda", "Justin"],
        "num_cups" => [0, 1, 2, 3]
    ].expect("Didnt read in df_a");
    
    let df_b: DataFrame = df![
        "age" => [16, 22, 25, 39],
        "names" => ["Jack", "Jonathan", "Aman", "Jussie"],
        "fav_food" => ["pasta", "candy", "sushi", "cake"]
    ].expect("Didnt read in df_b");

    // Create DataFrame 1
    let df1: DataFrame = df![
        "names1" => ["Alice", "Alicia", "Alex", "Bob", "Barbara", "Charlie", "Charlotte"],
        "age" => [25, 30, 35, 30, 35, 40, 45]
    ].expect("Didnt read in df_a");
   // Create DataFrame 2
    let df2: DataFrame = df![
        "names2" => ["Alice", "Alison", "Alexandra", "Bobby", "Babs", "Charles", "Charlene"],
        "favorite_number" => [25, 30, 35, 30, 35, 40, 45]
    ].expect("Didnt read in df2");

    let mut dataframemerger: DataFrameMerger = DataFrameMerger::new();
    // Will later be replaced by the struct fields: thresholds, hierarchies, and scorers
    fn string_comparer(s1: &str, s2: &str) -> f32 {
        if s1 == s2 {
            return 1.0;
        }
        0.0
    }
    use smartstring::*;
    //let owned_data: Vec<AnyValue> = df2
    //    .index("names2")
    //    .iter()
    //    .map(|s| match s {
    //        AnyValue::String(s) => {
    //            let mut converted_v = SmartString::new();
    //            converted_v.push_str(s);
    //            AnyValue::StringOwned(converted_v)
    //        },
    //        AnyValue::Null => AnyValue::Null, 
    //        _ => panic!("Incorrect dtype"),
    //    })
    //    .collect();
    dataframemerger.add_hierarchy(Some(col("favorite_number").gt_eq(30)), 0.45, fuzzy_compare);
    
    println!("{:#?}", dataframemerger.thresholds);
    println!("{:#?}", dataframemerger.scorers[0]("name", "names"));
    dataframemerger.merge(df1, df2, "names1", "names2", false);
    
}
