use polars_lazy::prelude::*;
use std::sync::mpsc;
use smartstring::*;
use polars::prelude::*; 
use polars::datatypes::*;
use rayon::prelude::*;
use std::collections::HashMap;
use std::time::{Instant, Duration};
use std::thread;
use std::iter::*;
use polars::frame::row::Row;
use std::ops::Index;
use itertools::{izip, enumerate};

pub struct DataFrameMerger{
    pub hierarchy: Vec<Expr>,
    pub thresholds: Vec<f32>,
    // !!! DESIGN CHOICE !!!
    // For our scorers, do we want their output to be from [0, 1] or from [0, 100]
    pub scorers: Vec<fn(&str, &str) -> f32>, // !!!NOTE TO SELF!!!: Look into difference between fn and Fn
}


impl DataFrameMerger {
    pub fn new() -> Self {
        DataFrameMerger { 
            hierarchy: Vec::new(), 
            thresholds: Vec::new(), 
            scorers: Vec::new()
        }
    }


    /// Adds a hierarchy to instance
    pub fn add_hierarchy(&mut self, filter: Option<Expr>, threshold: f32, scorer: fn(&str, &str) -> f32) {      
        // Define a default filter
        let filter: Expr = match filter {
            Some(x) => x,
            None => lit(true), 
        };
        println!("{:#?}", filter);
        
        self.hierarchy.push(filter);
        self.thresholds.push(threshold);
        self.scorers.push(scorer);
    }

    /// Allows user to set the hierarchy at a given idx
    pub fn set_hierarchy(&mut self, idx: usize, filter: Option<Expr>, threshold: f32, scorer: fn(&str, &str) -> f32) { 
        // Define a default filter
        let filter: Expr = match filter {
            Some(x) => x,
            None => lit(true), 
        };
        
        // !!! DESIGN CHOICE !!!
        // There are two options for functionality here w.r.t. `idx`
        // 1) if idx is out of bounds, we can default to add_hierarchy(**args)
        // 2) if idx is out of bounds, we can not run at all via panic!
        self.hierarchy[idx] = filter;
        self.thresholds[idx] = threshold;
        self.scorers[idx] = scorer;
    }

    // Need to implement copy for Row<>, Expr, and fn?

    ///
    pub fn clear_hierarchy(&mut self) { 
        self.hierarchy.clear();
        self.thresholds.clear();
        self.scorers.clear();
    }

    // Why 'static?
    pub fn merge(&self, data_frame: DataFrame, other: DataFrame, left_on: &'static str, right_on: &'static str, get_matches: bool) -> DataFrame { 
        // to improve merge speed, keep a track matched rows from_left_keys
        let mut seen: HashMap<&str, Row> = HashMap::new();

        let now = Instant::now();      
        // Spawn X threads  
        let mut df: DataFrame = DataFrame::empty();

        let mut builder_data_frame: Vec<polars::frame::row::Row<'_>> = vec![];
        // Does rayon crate make more sense here?
        thread::scope(|s| {
            s.spawn(|| {
                'outer: for left_key in data_frame.index(left_on).str().expect("Failed to get left_column").iter() {
                    // Handle the case when left_key is null
                    let left_key = match left_key {
                        Some(x) => x,
                        None => {
                            println!("left_key None");
                            // append an empty row to builder_data_frame
                            builder_data_frame.push(Row::new(Self::empty(other.width())));
                            continue
                        },
                    };                    
                    
                    // Check if left_key is inside the dictionary
                    if seen.contains_key(left_key) {
                        println!("left_key already seen {}", left_key);
                        builder_data_frame.push(seen.get(left_key).unwrap().clone());
                        continue
                    }

                    // Iterate through each hierarchy 
                    for (hierarchy, threshold, scorer) in izip!(&self.hierarchy, &self.thresholds, &self.scorers) {
                        println!("left_key {} from the spawned thread!", left_key);
                        // Filter a clone of the original data_frame
                        let filtered_data_frame = other
                            .clone()
                            .lazy()
                            .filter(hierarchy.clone())
                            .collect()
                            .unwrap();

                        let (match_index, match_score): (usize, f32) = filtered_data_frame
                            .index(right_on)
                            .iter()
                            .map(|s| match s {
                                AnyValue::String(s) => {
                                    scorer(s, left_key)
                                    },
                                AnyValue::Null => 0 as f32, 
                                _ => panic!("'right_on' is not a String or &str dtype"),
                            })
                            .enumerate()
                            .max_by(|(_, a), (_, b)| a.total_cmp(b))
                            .map(|(index, score)| (index, score))
                            .unwrap();
                        if match_score >= *threshold {
                            let row_to_append: Row<'_> = Row::new(filtered_data_frame.get_row(match_index).unwrap().0.iter().map(|v| v.clone().into_static().unwrap()).collect());
                            println!("Row to append {:#?}", row_to_append);
                            builder_data_frame.push(row_to_append);
                            continue 'outer;
                        }
                    }        
                    // If we do not get any matches for all the hierarchies, append a null row   
                    println!("Append null");
                    builder_data_frame.push(Row::new(Self::empty(other.width())));
                }
                println!("{:#?}", builder_data_frame);
                let data_frame_to_merge = DataFrame::from_rows_and_schema(&builder_data_frame[..], &other.schema()).expect("");
                println!("{:#?}", data_frame_to_merge);
                df = data_frame.hstack(data_frame_to_merge.get_columns()).expect("Failed to combine data-frames");
            });
        });
    
        let elapsed_time = now.elapsed();
        println!("{:?} Time passed", elapsed_time);

        // let output: DataFrame = handle.join().unwrap();                
        println!("Thread output: {:#?}", df);
        // Unpack the dataframe 
        df
    }

    fn empty<'a>(n: usize) -> Vec<polars::prelude::AnyValue<'a>> {
        (0..n).map(|_| AnyValue::Null).collect()
    }

}
