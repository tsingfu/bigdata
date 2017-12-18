tryCatch({
  lib_loc <- file.path("/opt/r4ml" , "R", "lib")
  .libPaths(c(lib_loc, .libPaths()))
  lib_loc <- file.path("/opt/spark" , "R", "lib")
  .libPaths(c(lib_loc, .libPaths()))
  library(R4ML)
  r4ml.session()
  r4ml.session.stop()
}, warnings =  function(w) ifelse(grepl("validateTransformOptions", w), quit(save="no", status=0), quit(save="no", status=1)),
error = function(e) {print(e); quit(save="no", status=2)})
quit(save="no", status=0)
