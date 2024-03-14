use polars::prelude::*;
use polars::df;
use polars_lazy::frame::*;
use polars_lazy::prelude::*;
use crate::dataframemerger::*;
use std::time::{Instant, Duration};
use std::thread;
use std::iter::*;
use std::cmp::Ordering;
use polars::frame::row::Row;
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
        "a" => [1, 2, 1, 1],
        "b" => ["a", "b", "c", "c"],
        "c" => [0, 1, 2, 3]
    ].expect("Didnt read in df_a");
    
    let df_b: DataFrame = df![
        "foo" => [1, 1, 1],
        "bar" => ["a", "c", "c"],
        "ham" => ["le", "var", "const"]
    ].expect("Didnt read in df_b");

    // Calling a slow function, it may take a while
    // slow_function();

    // df_a.hstack(data_frame_to_merge.get_columns());

    println!("{:#?}", df_a["d"]);
    // let mut dataframemerger: DataFrameMerger = DataFrameMerger::new();
    // dataframemerger.merge(df_a, df_b, "a", "bar");
}
