fn main() {
    let series = "

= = = =

survey abbreviation	=		JT
seasonal (code)		=		S
industry_code		=		000000
state_code		=		00
area_code		=		00000
sizeclass_code		=		00
dataelement_code	=		JO
ratelevel_code		=		R";

    let mut idx = 0;

    let series_meta = series
        .split('\n')
        .filter_map(|s| {
            let sv: Vec<&str> = s.split("=").collect();
            if sv.len() == 2 {
                idx += sv[1].trim().chars().count(); //where to insert a file delimter
                let ret = Some((sv[0].trim(), idx));
                idx += 1;
                ret
            } else {
                None
            }
        })
        .collect::<Vec<(&str, usize)>>();
    /* Clumsy
    let mut header = "".to_string();
    for i in 0..series_meta.len() - 1 {
        header.push_str(series_meta[i].0);
        header.push('\t');
    }
   */
   
    let h = series_meta
        .iter()
        .map(|c| c.0.replace(" ", "_").replace("(", "").replace(")", ""))
        .collect::<Vec<String>>()
        .join("\t");

    println!("{}", h);

    let mut s = String::with_capacity(21 + 8);
    s.push_str("JTS000000000000000JOR");

    for i in 0..series_meta.len() - 1 {
        s.insert(series_meta[i].1, '\t')
    }

    println!("{}", s);
}
