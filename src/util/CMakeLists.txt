add_library(
  Tskydb_util
  OBJECT
  hash.cc
)
set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:Tskydb_util>
  PARENT_SCOPE)
