add_library(
  Tskydb_lrucache
  OBJECT
  hashtable.cc
  lrucache.cc
)
set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:Tskydb_lrucache>
  PARENT_SCOPE)
