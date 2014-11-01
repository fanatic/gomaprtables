package gomaprtables

/*
#include <stdlib.h>
#include <hbase/hbase.h>
*/
import "C"
import "os"

type LogLevel int

const (
  INVALID LogLevel = C.HBASE_LOG_LEVEL_INVALID
  FATAL   LogLevel = C.HBASE_LOG_LEVEL_FATAL
  ERROR   LogLevel = C.HBASE_LOG_LEVEL_ERROR
  WARN    LogLevel = C.HBASE_LOG_LEVEL_WARN
  INFO    LogLevel = C.HBASE_LOG_LEVEL_INFO
  DEBUG   LogLevel = C.HBASE_LOG_LEVEL_DEBUG
  TRACE   LogLevel = C.HBASE_LOG_LEVEL_TRACE
)

// SetLogLevel sets the log output level in the hbase library
func SetLogLevel(level LogLevel) {
  //BUG: undefined reference to `hb_log_set_level'
  //C.hb_log_set_level((HBaseLogLevel)(level))
}

// SetLogStream changes the default output of logs from stderr to another
// file stream.  Set stream to nil to disable logging.
func SetLogStream(stream *os.File) {
  //BUG: undefined reference to `hb_log_set_stream'
  //C.hb_log_set_stream((FILE)(stream))
}
