use polars_lazy::prelude::*;
use polars::prelude::*; 
use polars::datatypes::*;
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
                for left_key in data_frame.index(left_on).str().expect("Failed to get left_column").iter() {
                    // Handle the case when left_key is null
                    let left_key = match left_key {
                        Some(x) => x,
                        None => {
                            // append an empty row to builder_data_frame
                            builder_data_frame.push(Row::new(Self::empty(other.width())));
                            continue
                        },
                    };                    
                    
                    // Check if left_key is inside the dictionary
                    if seen.contains_key(left_key) {
                        builder_data_frame.push(seen.get(left_key).unwrap().clone());
                        continue
                    }

                    // Iterate through each hierarchy 
                    for (hierarchy, threshold, scorer) in izip!(&self.hierarchy, &self.thresholds, &self.scorers) {
                        println!("left_key {} from the spawned thread!", left_key);
                        // APPLY hierarchies here
                        // Make sure that the scores are in order?
                        
                        // When searching for scores, try to leverage iterators as much as
                        // possible.
                        let scores: Vec<f32> = Self::get_match_scores(&other, left_key, right_on, hierarchy.clone(), *scorer);
                        println!("{:#?}", scores);
                        
                        let highest_match_score: f32 = scores.clone().into_iter().reduce(f32::max).unwrap();
                        if highest_match_score >= *threshold {
                            // How are we going to handle non-unique matches? 
                            // Also, my concern is that get_match_scores returns a filtered version
                            // of other
                            let right_index_to_merge: usize = scores
                                .iter()
                                .enumerate()
                                .max_by(|(_, a), (_, b)| a.total_cmp(b))
                                .map(|(index, _)| index)
                                .expect("Could not find maximum match in right index");
                            // Get the index of the other data-frame with the highest match score to the current key
                            
                            let matched_row = other.get_row(right_index_to_merge).expect("Failed to get row of `other` by index");
                            // Append the matched_row to the dictionary
                            seen.insert(left_key, matched_row);
                            // Push the row to our builder_data_frame
                            builder_data_frame.push(seen.get(left_key).unwrap().clone());
                            continue
                        } 
                        println!("NO MATCHES");
                        // If we do not get any matches for all the hierarchies, append a null row   

                        builder_data_frame.push(Row::new(Self::empty(other.width())));
                    }
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

    fn get_top_match() {

    }

    fn get_match_scores(dataframe: &DataFrame, left_key: &str, col_name: &str, screen: Expr, func: fn(&str, &str) -> f32) -> Vec<f32> {
        dataframe.clone()
            .lazy()
            .filter(screen)
            .collect()
            .expect("Expr was not correctly specified")
            .index(col_name)
            .str()
            .expect("Failed to get right_column")
            .iter()
            .map(|option_right_key| { 
                let output = match option_right_key {
                    None => 0 as f32,
                    Some(key) => func(left_key, key)
                };
                    output
                })
            .collect::<Vec<_>>()
    }
}
