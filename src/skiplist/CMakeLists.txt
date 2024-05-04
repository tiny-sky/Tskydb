add_library(
  Tskydb_skiplist
  OBJECT
  skiplist.cc
)
set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:Tskydb_skiplist>
  PARENT_SCOPE)
