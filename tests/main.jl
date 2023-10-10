# Import libraries
using Test

# Core code file
current_dir = @__DIR__
int_path = joinpath(current_dir,
                    "integration_test",
                    "integration_test.jl")
unit_path = joinpath(current_dir,
                    "unit_test", 
                    "unit_test.jl")

@testset "Main Test Suite" begin
    include(int_path)
    include(unit_path)
end