@@ -0,0 +1,35 @@
package mr

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const Debug = false

var file *os.File

func init() {
	rand.Seed(10)
	f, err := os.Create("log-" + strconv.Itoa(int(time.Now().Unix()+rand.Int63n(100))) + ".txt")
	if err != nil {
		DPrintf("log create file fail!")
	}
	file = f
}

//debug print log
func DPrintf(format string, value ...interface{}) {
	now := time.Now()
	info := fmt.Sprintf("%v-%v-%v %v:%v:%v:  ", now.Year(), int(now.Month()), now.Day(), now.Hour(), now.Minute(), now.Second()) + fmt.Sprintf(format+"\n", value...)

	if Debug {
		log.Printf(info)
	} else {
		file.WriteString(info)
	}
}