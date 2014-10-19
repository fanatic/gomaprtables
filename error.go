package gomaprtables

import "fmt"

type Errno int

func (e Errno) Error() string {
  s := errText[e]
  if s == "" {
    return fmt.Sprintf("errno %d", int(e))
  }
  return s
}
var errText = map[Errno]string{}
