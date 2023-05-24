file(REMOVE_RECURSE
  "libglogd.pdb"
  "libglogd.so"
  "libglogd.so.0.6.0"
  "libglogd.so.1"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/glog.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
