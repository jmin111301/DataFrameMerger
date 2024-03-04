use polars_lazy::prelude::*;

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
    pub fn get_hierarchy(&self, idx: usize) -> () {
        
    }

    /// 
    pub fn merge(&self) { 

    }
}