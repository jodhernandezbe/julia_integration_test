# Import packages
using Test
using JSON3
using DotMaps

# Core code file
current_dir = @__DIR__
src_path = joinpath(dirname(dirname(current_dir)),
                    "src",
                    "main.jl")

# Import modules
include(src_path)

@testset "Test for get_prtr_data function" begin
  query_result = Dict(:national_sector_code => [325110, 325110, 325110, 325110, 325110, 325110, 325110, 325110, 325110, 325110, 325110, 325110, 325110, 325110, 325110, 325110, 325110, 325110, 325110, 325110, 325110, 325110, 325110], 
       :reporting_year => [2016, 2016, 2016, 2016, 2016, 2016, 2016, 2016, 2016, 2016, 2016, 2016, 2016, 2016, 2016, 2016, 2016, 2016, 2016, 2016, 2016, 2016, 2016], 
       :generic_transfer_class_name => ["Landfill", "Energy recovery", "Energy recovery", "Energy recovery", "Destruction", "Destruction", "Destruction", "Destruction", "Destruction", "Destruction", "Destruction", "Destruction", "Destruction", "Other treatment", "Storage", "Sewerage", "Energy recovery", "Energy recovery", "Energy recovery", "Recycling", "Other treatment", "Other treatment", "Other treatment"], 
       :generic_substance_name => ["Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane"], 
       :transfer_amount_kg => [14.97, 340194.0, 4700.12, 5.26, 9.07, 3901.34, 6594.32, 684.02, 816.47, 2.09, 3218.69, 1.36, 12.66, 0.0, 1612.97, 0.0, 25854.74, 22189.72, 0.06, 226.8, 1157.11, 506.21, 56.7], 
       :reliability_score => [4, 4, 4, 1, 1, 2, 4, 4, 4, 4, 1, 1, 1, 1, 4, 2, 4, 1, 1, 4, 4, 1, 4])
  df = get_prtr_data(query_result)
  result = df[df.generic_transfer_class_name .== "Recycling",
              Cols(["avg_transfer_amount_kg"])][1,1]
  @test size(df) == (7, 8)
  @test result ≈ 226.8 atol=1 broken=false
end


@testset "Test for parse_census_data function" begin
  http_response = DotMap(Dict(:status => 200,
                              :body => UInt8[0x5b, 0x5b, 0x22, 0x4e, 0x41, 0x49, 0x43, 0x53, 
                                              0x5f, 0x54, 0x54, 0x4c, 0x22, 0x2c, 0x22, 0x45, 
                                              0x4d, 0x50, 0x22, 0x2c, 0x22, 0x47, 0x45, 0x4f, 
                                              0x5f, 0x54, 0x54, 0x4c, 0x22, 0x2c, 0x22, 0x74, 
                                              0x69, 0x6d, 0x65, 0x22, 0x2c, 0x22, 0x4e, 0x41, 
                                              0x49, 0x43, 0x53, 0x22, 0x2c, 0x22, 0x75, 0x73, 
                                              0x22, 0x5d, 0x2c, 0x0a, 0x5b, 0x22, 0x50, 0x65, 
                                              0x74, 0x72, 0x6f, 0x63, 0x68, 0x65, 0x6d, 0x69, 
                                              0x63, 0x61, 0x6c, 0x20, 0x6d, 0x61, 0x6e, 0x75, 
                                              0x66, 0x61, 0x63, 0x74, 0x75, 0x72, 0x69, 0x6e, 
                                              0x67, 0x22, 0x2c, 0x22, 0x39, 0x33, 0x37, 0x39, 
                                              0x22, 0x2c, 0x22, 0x55, 0x6e, 0x69, 0x74, 0x65, 
                                              0x64, 0x20, 0x53, 0x74, 0x61, 0x74, 0x65, 0x73, 
                                              0x22, 0x2c, 0x22, 0x32, 0x30, 0x31, 0x36, 0x22, 
                                              0x2c, 0x22, 0x33, 0x32, 0x35, 0x31, 0x31, 0x30, 
                                              0x22, 0x2c, 0x22, 0x31, 0x22, 0x5d, 0x5d]))
  employees = parse_census_data(http_response)
  @test employees == 9379
end

@testset "Test for submit_data function" begin
  employees = 9379
  example_dict = Dict(:national_sector_code => [325110, 325110, 325110, 325110, 325110, 325110, 325110], 
       :avg_reliability_score => [2.4444444444444446, 2.0, 2.5, 4.0, 2.5, 4.0, 4.0],
       :reporting_year => [2016, 2016, 2016, 2016, 2016, 2016, 2016],
       :generic_transfer_class_name => ["Destruction", "Sewerage", "Other treatment", "Recycling", "Energy recovery", "Landfill", "Storage"],
       :generic_substance_name => ["Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane", "Hexane"],
       :avg_transfer_amount_kg => [1693.3355555555556, 0.0, 430.005, 226.8, 65490.649999999994, 14.97, 1612.97],
       :min_transfer_amount_kg => [1.36, 0.0, 0.0, 226.8, 0.06, 14.97, 1612.97],
       :max_transfer_amount_kg => [6594.32, 0.0, 1157.11, 226.8, 340194.0, 14.97, 1612.97])
  df = DataFrame(example_dict)
  response = submit_data(employees, df)
  response_dict = copy(JSON3.read(response))

  external_keys = [:reporting_year, :national_sector_code,
                  :generic_substance_name, :value]
  internal_keys = [:avg_reliability_score, :max_transfer_amount_kg,
                  :min_transfer_amount_kg, :generic_transfer_class_name,
                  :avg_transfer_amount_kg]

  for key in external_keys
    @test key in collect(keys(response_dict))
    if key in collect(keys(response_dict)) && key == :value
      for key_1 in internal_keys
        @test key_1 in collect(keys(response_dict[:value][1]))
      end
    end
  end
end