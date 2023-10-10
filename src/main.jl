# Import libraries
using DBInterface
using SQLCipher
using DataFrames
using CSV
using HTTP
using DotEnv
using ArgParse
using Statistics

# Load environment variables from the .env file
DotEnv.config()

# Current folder
current_dir = @__DIR__

function open_db_session()

  # DB parameters
  password = "medium2023"
  db_path = src_path = joinpath(dirname(current_dir),
                                "data",
                                "PRTR_transfers_summary.db")

  # Connect to SQLCipher
  db = SQLCipher.DB(db_path)
  conn = DBInterface;
  SQLCipher.execute(db, """PRAGMA key="$password";""")
  
  return (conn, db)

end

function sql_query(conn, db, year, substance_id, naics)
  sql_query = """
  WITH aux_substance_tab(generic_substance_name,
				national_generic_substance_id) AS (
	SELECT generic_substance.generic_substance_name,
		national_generic_substance.national_generic_substance_id
	FROM generic_substance
	LEFT JOIN national_generic_substance ON generic_substance.generic_substance_id = national_generic_substance.generic_substance_id
	WHERE generic_substance.generic_substance_id = '$substance_id'
),
	aux_record_tab(reporting_year,
			 transfer_amount_kg,
			 reliability_score,
			 national_generic_substance_id,
			 national_generic_transfer_class_id,
			 national_facility_and_generic_sector_id) AS (
	SELECT transfer_record.reporting_year,
		transfer_record.transfer_amount_kg,
		transfer_record.reliability_score,
		transfer_record.national_generic_substance_id,
		transfer_record.national_generic_transfer_class_id,
		transfer_record.national_facility_and_generic_sector_id
	FROM transfer_record
	WHERE transfer_record.reporting_year = $year
), aux_transfer_tab(national_generic_transfer_class_id,
					generic_transfer_class_name) AS (
	SELECT
		national_generic_transfer_class.national_generic_transfer_class_id,
		generic_transfer_class.generic_transfer_class_name
	FROM national_generic_transfer_class
	INNER JOIN generic_transfer_class ON generic_transfer_class.generic_transfer_class_id = national_generic_transfer_class.generic_transfer_class_id
), aux_sector_tab(national_sector_code,
				national_facility_and_generic_sector_id) AS (
	SELECT
		national_sector.national_sector_code,
		facility.national_facility_and_generic_sector_id
	FROM national_sector
	JOIN national_generic_sector ON national_generic_sector.national_sector_id = national_sector.national_sector_id
	JOIN facility ON facility.national_generic_sector_id = national_generic_sector.national_generic_sector_id
	WHERE national_sector.industry_classification_system = 'USA_NAICS'
		AND national_sector.national_sector_code = $naics
)
SELECT aux_record_tab.reporting_year,
	aux_record_tab.transfer_amount_kg,
	aux_record_tab.reliability_score,
	aux_substance_tab.generic_substance_name,
	aux_transfer_tab.generic_transfer_class_name,
	aux_sector_tab.national_sector_code
FROM aux_substance_tab, aux_record_tab, aux_transfer_tab, aux_sector_tab
WHERE aux_substance_tab.national_generic_substance_id = aux_record_tab.national_generic_substance_id
	AND aux_transfer_tab.national_generic_transfer_class_id = aux_record_tab.national_generic_transfer_class_id
	AND aux_sector_tab.national_facility_and_generic_sector_id = aux_record_tab.national_facility_and_generic_sector_id
  """

  query_result = conn.execute(db, sql_query)
  
  return query_result
end

function get_prtr_data(query_result)
  
  df = DataFrame(query_result)

  df = combine(groupby(df, ["reporting_year",
                      "generic_substance_name",
                      "generic_transfer_class_name",
                      "national_sector_code"]),

              # Transfer amount
              "transfer_amount_kg" => ( x ->  (
                min_transfer_amount_kg=minimum( skipmissing(x), init=minimum(x) ),
                max_transfer_amount_kg=maximum( skipmissing(x), init=minimum(x) ),
                avg_transfer_amount_kg=mean(skipmissing(x))
              )  ) => AsTable,

              # Reliability
              "reliability_score" => x -> mean( skipmissing(x) )
            )

  rename!(df, :reliability_score_function => :avg_reliability_score)

  return df

end

function get_us_census_bureau_data(year,
                                  naics)

  api_key = ENV["API_KEY"]
  api_url = "https://api.census.gov/data/timeseries/asm/industry?get=NAICS_TTL,EMP,GEO_TTL&for=us:*&time=$year&NAICS=$naics&key=$api_key"

  response = HTTP.get(api_url)

  return response

end

using JSON3

function parse_census_data(http_response)

  employees = nothing
  try
    if http_response.status == 200
      data = JSON3.read(IOBuffer(String(http_response.body)))
      employees = parse(Int, data[2][2])
    end
  catch
    println("Something was wrong with the request")
  end
  return employees
end


function submit_data(employees, df)

  values_list = []
  columns_to_include = ["min_transfer_amount_kg",
                        "max_transfer_amount_kg",
                        "avg_transfer_amount_kg",
                        "avg_reliability_score",
                        "generic_transfer_class_name"]
  for row in eachrow(df)
    # Create a dictionary from the row and append it to the list
    row_dict = Dict(column => row[column] for column in columns_to_include)
    push!(values_list, row_dict)
  end

  response = Dict("reporting_year" => df[1, 1],
                  "generic_substance_name" => df[1, 2],
                  "national_sector_code" => df[1, 4],
                  "value" => values_list)

  json_string = JSON3.write(response, pretty=true)
  
  return json_string

end


function main(year, substance_id, naics)

  # Create SQLCipher connection
  conn, db = open_db_session()

  # Execute query
  query_result = sql_query(conn, db, year, substance_id, naics)

  # Query the Database
  df = get_prtr_data(query_result)

  # Query the US Census Bureau API
  http_response = get_us_census_bureau_data(year, naics)

  # Parse the data
  employees = parse_census_data(http_response)

  # Store data
  response = submit_data(employees, df)
  
  return response

end


function parse_commandline()

  parser = ArgParseSettings()

  @add_arg_table parser begin
      "--substance_id"
          help = "Substance ID to query"
          default = "110543"
          arg_type = String
      "--naics"
          help = "Sector code according to 2017 NAICS"
          default = 325110
          arg_type = Int
      "--year"
          help = "Year to query the PRTR data"
          default = 2016
          arg_type = Int
  end

  return parse_args(parser)
end

# Access the values of the parsed arguments
args = parse_commandline()
substance_id = args["substance_id"]
naics = args["naics"]
year = args["year"]

result = main(year, substance_id, naics)

println("Your answer is:\n\n$result\n\nThank you for using our service! ❤️")