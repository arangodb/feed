package operations

import (
	"github.com/arangodb/feed/pkg/config"
	"github.com/arangodb/feed/pkg/feedlang"

	"fmt"
	"sort"
	"strconv"
	"time"
)

// init sets up the various operations and links them to feedlang
func Init() {
	if feedlang.Atoms == nil {
		feedlang.Atoms = make(map[string]feedlang.Maker, 100)
	}
	feedlang.Atoms["normal"] = NewNormalProg
}

// CheckInt64Parameter is used to work on user input from the program and
// extract an integer parameter value.
func CheckInt64Parameter(value *int64, name string, input string) error {
	i, e := strconv.ParseInt(input, 10, 64)
	if e != nil {
		fmt.Printf("Could not parse %s argument to number: %s, error: %v\n", name, input, e)
		return e
	}
	*value = i
	return nil
}

// The following is used to sort durations:

type DurationSlice []time.Duration

func (d DurationSlice) Len() int {
	return len(d)
}

func (d DurationSlice) Less(a, b int) bool {
	return d[a] < d[b]
}

func (d DurationSlice) Swap(a, b int) {
	var dummy time.Duration = d[a]
	d[a] = d[b]
	d[b] = dummy
}

func WriteStatisticsForTimes(times []time.Duration, msg string) {
	sort.Sort(DurationSlice(times))
	var sum int64 = 0
	for _, t := range times {
		sum = sum + int64(t)
	}
	nr := int64(len(times))

	config.OutputMutex.Lock()
	fmt.Printf("Times for %s: %s (median), %s (90%%ile), %s (99%%ilie), %s (average)\n",
		msg,
		times[int(float64(0.5)*float64(nr))],
		times[int(float64(0.9)*float64(nr))],
		times[int(float64(0.99)*float64(nr))],
		time.Duration(sum/nr))
	config.OutputMutex.Unlock()
}
