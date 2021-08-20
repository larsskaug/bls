use rayon::prelude::*;
use std::collections::HashMap;
use std::time::Instant;
use regex::Regex;

fn main() {
    // from BLS documentation https://download.bls.gov/pub/time.series/jt/jt.txt
    let series = "
survey abbreviation    =        JT
seasonal (code)        =        S
industry_code        =        000000
state_code        =        00
area_code        =        00000
sizeclass_code        =        00
dataelement_code    =        JO
ratelevel_code        =        R";

    /*
    We want to use these column names and need the field lenght to do so.
    To that end, we will split the metadata by line
    where each line is turned into a tuple with the column name and byte count
    */
    let mut idx = 0;

    let series_meta = series
        .split('\n')
        // filter_map is ingenious, as it only returns data
        .filter_map(|s| {
            let sv: Vec<&str> = s.split("=").collect();
            if sv.len() == 2 {
                idx += sv[1].trim().chars().count(); //Position at which to insert a file delimiter
                let ret = Some((sv[0].trim(), idx));
                idx += 1;
                ret
            } else {
                None
            }
        })
        .collect::<Vec<(&str, usize)>>();

    let series_header = series_meta
        .iter()
        .map(|c| c.0.replace(" ", "_").replace("(", "").replace(")", ""))
        .collect::<Vec<String>>()
        .join("\t");

    println!("{}", series_header);

    fn tabulate(ins: &str, meta: &Vec<(&str, usize)>) -> String {
        let mut out = String::with_capacity(21 + 7); //21 before tabs are added. 8 elements, i.e., 7 tabs
        out.push_str(ins);

        for i in 0..meta.len() - 1 {
            out.insert(meta[i].1, '\t')
        }
        out
    }

    fn get_data(url: &str) -> Result<HashMap<&str, String>, reqwest::Error> {
        let text = reqwest::blocking::get(url)?.text()?;

        let newline = text.find('\n').unwrap();
        let mut data = HashMap::new();

        data.insert("header", text[..newline - 1].to_string());
        data.insert("rows", text[(newline + 1)..].to_string());
        Ok(data)
    }

    //let url = "https://download.bls.gov/pub/time.series/jt/jt.data.2.JobOpenings";
    let url = "https://download.bls.gov/pub/time.series/jt/jt.data.1.AllItems";

    let data = get_data(url).expect("Couldn't get data from url");

    let mut start = Instant::now();
    let overall_start = Instant::now();

    let expanded = data["rows"]
        .par_split('\n')//par_split is faster sometimes, never slower, but probably not worth it in this case.
        .filter_map(|row| {
            if row.chars().count() > 52 {
                Some(tabulate(&row[..21], &series_meta) + &row[21..53] + "\n")
            } else {
                None
            }
        })
        //.fold("", |i,j| {i.to_owned()+&j; i}); // Works just as well as collect
        .collect::<String>();

    println!("Expanded the data in {:?}", start.elapsed());

    start = Instant::now();

    std::fs::write("/home/lars/data/jolts.tsv", expanded).expect("Unable to write file");

    println!("Wrote the file in {:?}", start.elapsed());

    println!(
        "Everything except downloading data {:?}",
        overall_start.elapsed()
    );

    let re = Regex::new(r"(?P<abv>JT)(?P<sea>[S|U])(?P<ind>[0-9]{6})(?P<sta>[0-9]{2})(?P<ara>[0-9]{5})(?P<sze>[0-9]{2})(?P<del>[A-Z]{2})(?P<rat>[A-Z]{1})(?P<rst>.*)").unwrap();
    
    fn tabulate_re(ins: &str, re: &regex::Regex) -> String {
     re.replace(&ins, "$abv\t$sea\t$ind\t$sta\t$ara\t$sze\t$del\t$rat\t$rst").to_string()
    }
    

    start = Instant::now();

    let expanded_re = data["rows"]
        .par_split('\n')//This is more work and works far faster with par_splitcd ..
        .filter_map(|row| {
            if row.chars().count() > 52 {
                Some(tabulate_re(&row[..21], &re) + &row[21..53] + "\n")
            } else {
                None
            }
        })
        //.fold("", |i,j| {i.to_owned()+&j; i}); // Works just as well as collect
        .collect::<String>();         

    println!("Expanded with regex in {:?}", start.elapsed());

    println!("{}", expanded_re.chars().count());
    
    

}

