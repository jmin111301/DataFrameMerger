use polars_lazy::prelude::*;
use polars::prelude::*; 

use std::time::{Instant, Duration};
use std::thread;
use std::iter::*;
use polars::frame::row::Row;

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

    /// Don't really need this anymore since rust is safe!
    fn valid_hierarchy(&self) { }

    /// Adds a hierarchy to instance
    pub fn add_hierarchy(&mut self, filter: Option<Expr>, threshold: f32, scorer: fn(&str, &str) -> f32) {      
        // Define a default filter
        let filter: Expr = match filter {
            Some(x) => x,
            None => polars_lazy::dsl::all(),
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
            None => polars_lazy::dsl::all(),
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

    ///
    pub fn merge(&mut self, data_frame: DataFrame, other: DataFrame, left_on: usize, right_on: usize) -> DataFrame { 
        let now = Instant::now();

        // Will later be replaced by the struct fields: thresholds, hierarchies, and scorers
        let threshold = 0.75; 
        fn string_comparer(s1: &str, s2: &str) -> f32 {
            (((s1.len() + s2.len()) as f32)/(2 as f32)) as f32
        }

        let handle = thread::spawn(move || {
            let mut builder_data_frame: Vec<polars::frame::row::Row<'_>> = vec![];
            for left_key in data_frame[left_on].str().expect("Failed to get left_column").iter() {
                // need to handle None case for left column
                println!("left_key {} from the spawned thread!", left_key.unwrap());
                
                let scores: Vec<f32> = other[right_on]
                    .str()
                    .expect("Failed to get right_column")
                    .iter()
                    .map(|option_right_key| { 
                        let output = match option_right_key {
                            None => 0 as f32,
                            Some(key) => string_comparer(left_key.unwrap(), key)
                        };
                        output
                    })
                    .collect();

                let index_of_max: Option<usize> = scores
                    .iter()
                    .enumerate()
                    .max_by(|(_, a), (_, b)| a.total_cmp(b))
                    .map(|(index, _)| index);
                    
                let right_index_to_merge = index_of_max.expect("Unable to identify max");
                let highest_match_score: f32 = scores.into_iter().reduce(f32::max).unwrap();

                // APPLY hierarchies here
                
                let row_to_append = if highest_match_score >= threshold {
                    // Get the index of the other data-frame with the highest match score to the current key
                    other.get_row(right_index_to_merge).expect("Failed to get the row by index")
                } else {
                    // Add a row of nulls
                    Row::new(vec![])
                };
                builder_data_frame.push(row_to_append);

            }

            // Create the right data frame
            let data_frame_to_merge = DataFrame::from_rows_and_schema(&builder_data_frame[..], &other.schema()).expect("");
            data_frame.hstack(data_frame_to_merge.get_columns()).expect("Failed to combine data-frames")
        });
        let elapsed_time = now.elapsed();
        println!("{:?} time passed", elapsed_time);

        let output = handle.join().unwrap();
        println!("Thread output: {:#?}", output);
        // Unpack the dataframe 
        output
    }
}
