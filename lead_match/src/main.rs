use polars::prelude::*;
use std::error::Error;
use std::fmt;
use std::thread;
use serde::{Serialize};

fn read_csv(file_path: &str) -> Result<DataFrame, Box<dyn Error>> {
    let df = CsvReader::from_path(file_path)?.finish()?;
    Ok(df)
}


#[derive(Debug, Clone)]
struct Person {
    first_name: String,
    last_name: String,
    street_address: String,
    zip_code: i64,
    phone_number: String,
    ip_address: String,
    email: String,
    customer_id: i64,
}


// Function to calculate the minimum of three integers
fn min(a: usize, b: usize, c: usize) -> usize {
    if a <= b && a <= c {
        a
    } else if b <= a && b <= c {
        b
    } else {
        c
    }
}

fn levenshtein_distance(s1: &str, s2: &str) -> f64 {
    let s1 = s1.to_lowercase();
    let s2 = s2.to_lowercase();

    let len1 = s1.len();
    let len2 = s2.len();

    let mut matrix: Vec<Vec<usize>> = vec![vec![0; len2 + 1]; len1 + 1];

    for i in 0..=len1 {
        for j in 0..=len2 {
            if i == 0 {
                matrix[i][j] = j;
            } else if j == 0 {
                matrix[i][j] = i;
            } else {
                let substitution_cost = if s1.chars().nth(i - 1) == s2.chars().nth(j - 1) {
                    0
                } else {
                    1
                };
                matrix[i][j] = min(
                    matrix[i - 1][j] + 1,         // Deletion
                    matrix[i][j - 1] + 1,         // Insertion
                    matrix[i - 1][j - 1] + substitution_cost, // Substitution
                );
            }
        }
    }

    let distance = matrix[len1][len2] as f64;

    // Calculate the maximum possible distance (length of the longer string)
    let max_possible_distance = len1.max(len2) as f64;

    // Calculate the similarity ratio
    1.0 - distance / max_possible_distance
}



impl fmt::Display for Person {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Person {{ firstname: {}, lastname: {}, email: {}, address: {}, phonenumber: {} }}",
            self.first_name, self.last_name, self.email, self.street_address, self.phone_number
        )
    }
}

fn get_string_column(df: &DataFrame, column_name: &str, row: usize) -> Result<String, Box<dyn Error>> {
    let column = df.column(column_name)?.utf8()?;
    match column.get(row) {
        Some(value) => Ok(value.to_string()),
        None => Ok("".to_string()), // Return an empty string for null values
    }
}

fn get_int_column(df: &DataFrame, column_name: &str, row: usize) -> Result<i64, Box<dyn Error>> {
    let column = df.column(column_name)?.i64()?;
    match column.get(row) {
        Some(value) => Ok(value),
        None => Ok(0), // Return 0 for null values (or choose another default value)
    }
}

fn process_leads(file_path: &str) -> Result<Vec<Person>, Box<dyn Error>> {
    let df = read_csv(file_path)?;
    let mut people = Vec::new();
    for row in 0..df.height() {
        let first_name = get_string_column(&df, "First Name", row)?;
        let last_name = get_string_column(&df, "Last Name", row)?;
        let street_address = get_string_column(&df, "Street Address", row)?;
        let zip_code = get_int_column(&df, "Zip Code", row)?;
        let phone_number = get_string_column(&df, "Phone Number", row)?;
        let ip_address = get_string_column(&df, "IP Address", row)?;
        let email = get_string_column(&df, "Email", row)?;
        let customer_id = get_int_column(&df, "Customer ID", row)?;

        // Create a Person struct and add it to the Vec
        let person = Person {
            first_name,
            last_name,
            street_address,
            zip_code,
            phone_number,
            ip_address,
            email,
            customer_id,
        };

        people.push(person);
    }

    Ok(people)
}

fn process_csv_files(
    new_leads_path: &str,
    existing_leads_path: &str,
    thread_number: usize,
) -> Result<(), Box<dyn Error>> {
    let new_leads = process_leads(new_leads_path)?;
    let existing_leads = process_leads(existing_leads_path)?;

    let mut matches_indices: Vec<usize> = Vec::new();
    let mut no_matches_indices: Vec<usize> = Vec::new();

    for (new_idx, new_person) in new_leads.iter().enumerate() {
        // print!("Thread {}: Processed {} \n",thread_number, new_idx);
        let mut best_score = -1.0;
        let mut best_person = "";
        let mut best_index = None;

        for (existing_idx, existing_person) in existing_leads.iter().enumerate() {
            // Calculate Levenshtein distance for each field
            let firstname_score =
                levenshtein_distance(&new_person.first_name, &existing_person.first_name);
            let lastname_score =
                levenshtein_distance(&new_person.last_name, &existing_person.last_name);
            let phonenumber_score =
                levenshtein_distance(&new_person.phone_number, &existing_person.phone_number);
            let address_score =
                levenshtein_distance(&new_person.street_address, &existing_person.street_address);
            let email_score =
                levenshtein_distance(&new_person.email, &existing_person.email);
            let zipcode_score =
                levenshtein_distance(&new_person.zip_code.to_string(), &existing_person.zip_code.to_string());
            // Define weight categories
            let firstname_weight = 3;
            let lastname_weight = 5;
            let phonenumber_weight = 7;
            let address_weight = 5;
            let zipcode_weight = 2;
            let email_weight = 7;

            let weight_denominator =
                firstname_weight
                + lastname_weight
                + phonenumber_weight
                + address_weight
                + zipcode_weight
                + email_weight;

            // Calculate weighted score for each field
            let firstname_weighted_score = firstname_score * firstname_weight as f64;
            let lastname_weighted_score = lastname_score * lastname_weight as f64;
            let phonenumber_weighted_score = phonenumber_score * phonenumber_weight as f64;
            let address_weighted_score = address_score * address_weight as f64;
            let email_weighted_score = email_score * email_weight as f64;
            let zipcode_weighted_score = zipcode_score * zipcode_weight as f64;

            // Calculate total weighted score
            let total_weighted_score = firstname_weighted_score
                + lastname_weighted_score
                + phonenumber_weighted_score
                + address_weighted_score
                + email_weighted_score
                + zipcode_weighted_score;

            // Calculate final score
            let final_score = total_weighted_score / weight_denominator as f64;

            // println!(
            //     "Person {} vs Person {}: Final Score: {:.3}",
            //     new_idx, existing_idx, final_score
            // );

            if final_score > best_score {
                best_score = final_score;
                best_person = &existing_person.first_name;
                best_index = Some(new_idx);
            }
        }

        if best_score > 0.90 {
            if let Some(index) = best_index {
                matches_indices.push(index);
            }
            // println!("*********************************");
            // println!(
            //     "Person Matches {}: {:.3} (Person {})",
            //     new_idx, best_score, best_person
            // );
            // println!("*********************************");
        } else {
            if let Some(index) = best_index {
                no_matches_indices.push(index);
            }
            // println!("*********************************");
            // println!(
            //     "No matches, new person: {}: {:.3} (Person {})",
            //     new_idx, best_score, best_person
            // );
            // println!("*********************************");
        }
    }
    
    #[derive(Debug, Serialize)] // Add the Serialize attribute if you intend to serialize it to JSON
    struct MatchResult {
        matches: Vec<usize>,
        no_matches: Vec<usize>,
    }


    // Serialize the result as JSON
    let result_json_1 = serde_json::to_string(&matches_indices)?;
    let result_json_2 = serde_json::to_string(&no_matches_indices)?;

    print!("matches:{}|",result_json_1);
    print!("no_matches:{}|",result_json_2);
    

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let new_leads_path = "new.csv";
    let existing_paths = vec![
        "existing_1.csv",
        "existing_2.csv",
        "existing_3.csv",
        "existing_4.csv",
        // "existing_5.csv",
        // "existing_6.csv",
        // "existing_7.csv",
        // "existing_8.csv",
    ];

    // Create a vector to store thread handles
    let mut thread_handles = vec![];

    // Iterate over the existing paths and spawn a thread for each one
    for (thread_number, existing_path) in existing_paths.iter().enumerate() {
        let new_leads_path_clone = new_leads_path.to_string();
        let existing_path_clone = existing_path.to_string();

        // Spawn a thread
        let handle = thread::spawn(move || {
            process_csv_files(&new_leads_path_clone, &existing_path_clone, thread_number).unwrap();
        });

        // Store the thread handle
        thread_handles.push(handle);
    }

    // Wait for all threads to finish
    for handle in thread_handles {
        handle.join().unwrap();
    }

    Ok(())
}