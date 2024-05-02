-- Create intermediate table: Crime Hotspots by Borough and Location Type
CREATE TABLE
  `crime_data.crime_hotspots_by_borough` AS
SELECT
  BORO_NM AS borough,
  PREM_TYP_DESC AS location_type,
  COUNT(*) AS crime_count
FROM
  `crime_data.nyc_crime`
WHERE
  PREM_TYP_DESC IS NOT NULL
GROUP BY
  borough,
  location_type
ORDER BY
  crime_count DESC;

-- Create intermediate table: Yearly Distribution of Crime Categories
CREATE TABLE
  `crime_data.crime_distribution_by_year` AS
SELECT
  EXTRACT(YEAR
  FROM
    CMPLNT_FR_DT) AS year,
  OFNS_DESC AS offense_category,
  COUNT(*) AS crime_count
FROM
  `crime_data.nyc_crime`
GROUP BY
  year,
  offense_category
ORDER BY
  crime_count DESC;
