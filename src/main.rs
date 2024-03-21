use polars::prelude::*;
use polars::df;
use crate::dataframemerger::*;
use std::time::{Instant};
use std::ops::Index;
use std::cmp;
use std::thread;
use rayon::iter::*;
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
        "num_cars" => [1, 1, 1, 2],
        "names" => ["Jack", "Jonathan", "Aman", "Jussie"],
        "fav_food" => ["pasta", "candy", "sushi", "cake"]
    ].expect("Didnt read in df_b");

    let mut dataframemerger: DataFrameMerger = DataFrameMerger::new();
    // Will later be replaced by the struct fields: thresholds, hierarchies, and scorers
    fn string_comparer(s1: &str, s2: &str) -> f32 {
        (1.0 -  (((s1.len() as i32) - (s2.len() as i32)) as f32).abs()/(cmp::max(s1.len(), s2.len()) as f32)) as f32
    }

    dataframemerger.add_hierarchy(None, 0.75, string_comparer);
    println!("{:#?}", dataframemerger.thresholds);

    println!("{:#?}", dataframemerger.scorers[0]("name", "names"));
    dataframemerger.merge(df_a, df_b, "name", "names");
    
}
