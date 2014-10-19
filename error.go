package gomaprtables

import "fmt"

//Errno represents an HBase errorcode
type Errno int

//Error returns the error representation of the HBase error
func (e Errno) Error() string {
  s := errText[e]
  if s == "" {
    return fmt.Sprintf("errno %d", int(e))
  }
  return s
}

var errText = map[Errno]string{}
