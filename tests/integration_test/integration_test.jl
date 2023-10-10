# Import packages
using Test
using Base.Filesystem
using DBInterface
using DataFrames
using EzXML
using HTTP

# Core code file
current_dir = @__DIR__
src_path = joinpath(dirname(dirname(current_dir)),
                    "src",
                    "main.jl")

# Import modules
include(src_path)

@testset "Database Connection Tests" begin
  # Test if the database connection is established successfully
  result = 1
  try
    conn, db = open_db_session()
    conn.execute(db, "SELECT * FROM prtr_system;")
  catch SQLiteException
    result = 0
  end
  @test result == 1
end

function contains_error_message(html_content)
  etree = parsehtml(html_content)
  primates = root(etree)
  title_element = nodecontent.(findall("//title", primates))
  if length(title_element) !== 0 && title_element[1] == "Invalid Key"
    return true
  end
  return false
end

@testset "API Connection Test" begin
  # Test if the API connection is established successfully
  result = 1
  try 
    http_response = get_us_census_bureau_data("2016",
                                    "31-33")
    html_text = String(http_response.body)
    if contains_error_message(html_text)
      result = -1 # No valid key
    end
  catch
    result = 0 # No DNS or Service
  end
  @test result == 1
end