include(FetchContent)
FetchContent_Declare(
  googletest

  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
# Set to OFF to prevent downloading libgmock
set(BUILD_GMOCK OFF CACHE BOOL "Builds the googlemock subproject" FORCE)

FetchContent_MakeAvailable(googletest)