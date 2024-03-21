use polars_lazy::prelude::*;
use polars::prelude::*; 

use std::time::{Instant, Duration};
use std::thread;
use std::iter::*;
use polars::frame::row::Row;
use std::ops::Index;
use itertools::{izip, enumerate};

pub struct DataFrameMerger<'a>{
    pub hierarchy: Vec<&'a Expr>,
    pub thresholds: Vec<f32>,
    // !!! DESIGN CHOICE !!!
    // For our scorers, do we want their output to be from [0, 1] or from [0, 100]
    pub scorers: Vec<&'a fn(&str, &str) -> f32>, // !!!NOTE TO SELF!!!: Look into difference between fn and Fn
}


impl<'a> DataFrameMerger<'a> {
    pub fn new() -> Self {
        DataFrameMerger { 
            hierarchy: Vec::new(), 
            thresholds: Vec::new(), 
            scorers: Vec::new()
        }
    }

    /// Adds a hierarchy to instance
    pub fn add_hierarchy(&mut self, filter: Option<&Expr>, threshold: f32, scorer: &fn(&str, &str) -> f32) {      
        // Define a default filter
        let filter: &'a Expr = match filter {
            Some(x) => x,
            None => &polars_lazy::dsl::all(),
        };
        
        self.hierarchy.push(filter);
        self.thresholds.push(threshold);
        self.scorers.push(scorer);
    }

    /// Allows user to set the hierarchy at a given idx
    pub fn set_hierarchy(&mut self, idx: usize, filter: Option<&Expr>, threshold: f32, scorer: &fn(&str, &str) -> f32) { 
        // Define a default filter
        let filter: &'a Expr = match filter {
            Some(x) => x,
            None => &polars_lazy::dsl::all(),
        };
        
        // !!! DESIGN CHOICE !!!
        // There are two options for functionality here w.r.t. `idx`
        // 1) if idx is out of bounds, we can default to add_hierarchy(**args)
        // 2) if idx is out of bounds, we can not run at all via panic!
        self.hierarchy[idx] = filter;
        self.thresholds[idx] = threshold;
        self.scorers[idx] = scorer;
    }

    ///
    pub fn clear_hierarchy(&mut self) { 
        self.hierarchy.clear();
        self.thresholds.clear();
        self.scorers.clear();
    }

    // , num_threads: &'static usize
    // Potential design to make this whole fking thing work
    // merge(), we pass in variables from the struct. 

    pub fn merge(&self, data_frame: DataFrame, other: DataFrame, left_on: &'static str, right_on: &'static str) -> DataFrame { 
        let now = Instant::now();      
        // Spawn X threads  
        let mut df: DataFrame = DataFrame::empty();
        // Issue is with the thread??? 

        let mut builder_data_frame: Vec<polars::frame::row::Row<'_>> = vec![];

        thread::scope(|s| {
            s.spawn(move || {
                // Iterate through the 
                for (i, (hierarchy, threshold, scorer)) in enumerate(izip!(&self.hierarchy, &self.thresholds, &self.scorers)) {
                    println!("{}", i);
                    for left_key in data_frame.index(left_on).str().expect("Failed to get left_column").iter() {
                        // Handle the case when left_key is null
                        let left_key = match left_key {
                            Some(x) => x,
                            None => continue,
                        };
                        
                        println!("left_key {} from the spawned thread!", left_key);
                        // APPLY hierarchies here
                        // Make sure that the scores are in order?
                        let scores: Vec<f32> = other.clone()
                            .lazy()
                            .filter(*self.hierarchy[i])
                            .collect()
                            .expect("Failed to read in DataFrame")
                            .index(right_on)
                            .str()
                            .expect("Failed to get right_column")
                            .iter()
                            .map(|option_right_key| { 
                                let output = match option_right_key {
                                    None => 0 as f32,
                                    Some(key) => scorer(left_key, key)
                                };
                                output
                            })
                            .collect();

                        println!("{:#?}", scores);
                        
                        let index_of_max: Option<usize> = scores
                            .iter()
                            .enumerate()
                            .max_by(|(_, a), (_, b)| a.total_cmp(b))
                            .map(|(index, _)| index);
                            
                        let right_index_to_merge = index_of_max.expect("Unable to identify max");
                        let highest_match_score: f32 = scores.into_iter().reduce(f32::max).unwrap();
                        
                        if highest_match_score >= *threshold {
                            // Get the index of the other data-frame with the highest match score to the current key
                            builder_data_frame.push(other.get_row(right_index_to_merge).expect("Failed to get the row by index"));
                            // Need to break out of the inner loop, not the outer
                        } else if i == self.thresholds.len() - 1 {
                            // Add a row of nulls
                            builder_data_frame.push(Row::new(vec![]));
                        };
                    }
                }
                let data_frame_to_merge = DataFrame::from_rows_and_schema(&builder_data_frame[..], &other.schema()).expect("");
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
}
