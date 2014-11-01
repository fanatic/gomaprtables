package gomaprtables

import "fmt"

//Errno represents an HBase errorcode
type Errno int

//Error returns the error representation of the HBase error
func (e Errno) Error() string {
  s := errText[e]
  if s == "" {
    return fmt.Sprintf("errno=%d", int(e))
  }
  return s
}

var (
  ErrAgain            = Errno(11)         /* Try again */
  ErrNoBufs           = Errno(105)        /* No buffer space available */
  ErrNoEnt            = Errno(2)          /* No such table or column family */
  ErrExist            = Errno(6)          /* Table or column family already exists */
  ErrNoMem            = Errno(12)         /* Out of memory */
  ErrInternal         = Errno(-10000)     /* Internal error */
  ErrTableDisabled    = Errno(-10000 - 1) /* Table is disabled */
  ErrTableNotDisabled = Errno(-10000 - 2) /* Table is not disabled */
  ErrUnknownScanner   = Errno(-10000 - 3) /* Scanner does not exist on region server */
)
var errText = map[Errno]string{
  ErrAgain:            "Try again",
  ErrNoBufs:           "No buffer space available",
  ErrNoEnt:            "No such table or column family",
  ErrExist:            "Table or column family already exists",
  ErrNoMem:            "Out of memory",
  ErrInternal:         "Internal error",
  ErrTableDisabled:    "Table is disabled",
  ErrTableNotDisabled: "Table is not disabled",
  ErrUnknownScanner:   "Scanner does not exist on region server",
}
