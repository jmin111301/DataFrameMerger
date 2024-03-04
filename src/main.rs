use polars::prelude::*;
use polars::df;
use polars_lazy::frame::*;
use polars_lazy::prelude::*;
use crate::dataframemerger::*;
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
//            similarity score (floating point between 0 and 1, inclusive)

fn main() {
    let df_a: DataFrame = df![
        "a" => [1, 2, 1, 1],
        "b" => ["a", "b", "c", "c"],
        "c" => [0, 1, 2, 3]
    ].expect("Didnt read in df_a");
    
    let df_b: DataFrame = df![
        "foo" => [1, 1, 1],
        "bar" => ["a", "c", "c"],
        "ham" => ["let", "var", "const"]
    ].expect("Didnt read in df_b");

    let s1: String = String::from("hello");
    let s2: String =  String::from("world");

    test_scorer(&s1, &s2);

    println!("{:?}", df_a);

    let df_b = df_b.lazy()
    .group_by([col("bar")])
    .agg([
        col("foo").count().alias("foo_count"),
    ])
    .collect();
    
    println!("{:?}", df_b);

    fn test_scorer(string1: &str, string2: &str) -> f32 {
        println!("{} is string 1 and {} is string 2", string1, string2);
        1.0
    };

    let temp = test_scorer;

    let data_frame_merger = DataFrameMerger::new();

    let data_frame_merger = DataFrameMerger { 
        hierarchy: Vec::new(), 
        thresholds: Vec::new(), 
        scorers: vec![temp],
    };

    for func in data_frame_merger.scorers {
        func("hi", "me");
    }

    // let list_of_strings: Vec<String> =
    // list_of_numbers.iter().map(ToString::to_string).collect();
    let mut x = vec![1, 2, 3];
    println!("{:?}", x);
    x[4] = 2;
    println!("{:?}", x);

}
