use crate::CovidRecord;

pub fn remap_territories(record: &mut CovidRecord) {
    let new_country_region = match record.province_state.as_ref().map(|x| x.as_str()) {
        Some("Saint Barthelemy") => Some("France - Saint Barthelemy"),
        Some("St Martin") => Some("France - St Martin"),
        Some("French Polynesia") => Some("France - French Polynesia"),
        Some("French Guiana") => Some("France - French Guiana"),
        Some("Mayotte") => Some("France - Mayotte"),
        Some("Guadeloupe") => Some("France - Guadeloupe"),
        Some("Curacao") => Some("Netherlands - Curacao"),
        Some("Gibraltar") => Some("United Kingdom - Gibraltar"),
        Some("Cayman Islands") => Some("United Kingdom - Cayman Islands"),
        _ => None,
    };

    if let Some(new_country_region) = new_country_region {
        record.country_region = new_country_region.to_string();
    }

    let rename_country_region = match record.country_region.as_str() {
        "Antigua and Barbuda" => Some("Antigua & Barbuda"),
        "Bosnia and Herzegovina" => Some("Bosnia & Herzegovina"),
        "Saint Vincent and the Grenadines" => Some("Saint Vincent & the Grenadines"),
        "Trinidad and Tobago" => Some("Trinidad & Tobago"),
        _ => None,
    };

    if let Some(country_region) = new_country_region {
        record.country_region = country_region.to_string();
    }
}

// City, County, State
pub fn extract_us_data(col1: &str) -> (Option<String>, Option<String>, Option<String>) {
    if col1 == "Washington, D.C." {
        return (Some("Washington, D.C.".to_string()), None, None);
    } else if col1 == "Virgin Islands, U.S." {
        return (None, None, Some("Virgin Islands".to_string()));
    }

    if col1.contains(",") {
        // county or city
        if col1.contains("County") {
            // county
            let county_parts = col1.split(", ").collect::<Vec<&str>>();
            let county = county_parts[0].replace(" County", "");
            let state = county_parts[1];
            let state = convert_state_abbreviation_to_full(state.trim());
            (None, Some(county), Some(state.to_string()))
        } else {
            let city_parts = col1.split(", ").collect::<Vec<&str>>();
            let city = city_parts[0];
            let state = city_parts[1];
            let state = convert_state_abbreviation_to_full(state);
            (Some(city.to_string()), None, Some(state.to_string()))
        }
    } else {
        // state
        (None, None, Some(col1.to_string()))
    }
}

fn convert_state_abbreviation_to_full(abbrev: &str) -> &'static str {
    match abbrev {
        "AL" => "Alabama",
        "AK" => "Alaska",
        "AZ" => "Arizona",
        "AR" => "Arkansas",
        "CA" => "California",
        "CO" => "Colorado",
        "CT" => "Connecticut",
        "DE" => "Delaware",
        "FL" => "Florida",
        "GA" => "Georgia",
        "HI" => "Hawaii",
        "ID" => "Idaho",
        "IL" => "Illinois",
        "IN" => "Indiana",
        "IA" => "Iowa",
        "KS" => "Kansas",
        "KY" => "Kentucky",
        "LA" => "Louisiana",
        "ME" => "Maine",
        "MD" => "Maryland",
        "MA" => "Massachusetts",
        "MI" => "Michigan",
        "MN" => "Minnesota",
        "MS" => "Mississippi",
        "MO" => "Missouri",
        "MT" => "Montana",
        "NE" => "Nebraska",
        "NV" => "Nevada",
        "NH" => "New Hampshire",
        "NJ" => "New Jersey",
        "NM" => "New Mexico",
        "NY" => "New York",
        "NC" => "North Carolina",
        "ND" => "North Dakota",
        "OH" => "Ohio",
        "OK" => "Oklahoma",
        "OR" => "Oregon",
        "PA" => "Pennsylvania",
        "RI" => "Rhode Island",
        "SC" => "South Carolina",
        "SD" => "South Dakota",
        "TN" => "Tennessee",
        "TX" => "Texas",
        "UT" => "Utah",
        "VT" => "Vermont",
        "VA" => "Virginia",
        "WA" => "Washington",
        "WV" => "West Virginia",
        "WI" => "Wisconsin",
        "WY" => "Wyoming",
        unknown => panic!("`{}` not a state", unknown),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_state_extraction() {
        let result = extract_us_data("California");
        assert_eq!(result, (None, None, Some("California".to_string())))
    }

    #[test]
    fn test_city_extraction() {
        let result = extract_us_data("Los Angeles, CA");
        assert_eq!(
            result,
            (
                Some("Los Angeles".to_string()),
                None,
                Some("California".to_string())
            )
        )
    }

    #[test]
    fn test_county_extraction() {
        let result = extract_us_data("Shasta County, CA");
        assert_eq!(
            result,
            (
                None,
                Some("Shasta".to_string()),
                Some("California".to_string())
            )
        )
    }

    #[test]
    fn test_washington_dc_extraction() {
        let result = extract_us_data("Washington, D.C.");
        assert_eq!(result, (Some("Washington, D.C.".to_string()), None, None))
    }
}
