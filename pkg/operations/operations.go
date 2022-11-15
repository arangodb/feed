package operations

import (
	"github.com/arangodb/feed/pkg/config"
	"github.com/arangodb/feed/pkg/feedlang"

	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// init sets up the various operations and links them to feedlang
func Init() {
	if feedlang.Atoms == nil {
		feedlang.Atoms = make(map[string]feedlang.Maker, 100)
	}
	feedlang.Atoms["normal"] = NewNormalProg
	feedlang.Atoms["graph"] = NewGraphProg
}

// CheckInt64Parameter is used to work on user input from the program and
// extract an integer parameter value.
func CheckInt64Parameter(value *int64, name string, input string) error {
	var mult int64 = 1
	l := len(input)
	if l > 0 {
		if input[l-1] == 'G' || input[l-1] == 'g' {
			input = input[0 : l-1]
			mult = 1024 * 1024 * 1024
		} else if input[l-1] == 'T' || input[l-1] == 't' {
			input = input[0 : l-1]
			mult = 1024 * 1024 * 1024 * 1024
		} else if input[l-1] == 'M' || input[l-1] == 'm' {
			input = input[0 : l-1]
			mult = 1024 * 1024
		} else if input[l-1] == 'K' || input[l-1] == 'k' {
			input = input[0 : l-1]
			mult = 1024
		}
	}
	i, e := strconv.ParseInt(input, 10, 64)
	if e != nil {
		fmt.Printf("Could not parse %s argument to number: %s, error: %v\n", name, input, e)
		return e
	}
	*value = i * mult
	return nil
}

// GetInt64Value is used to work on parsed user input from the program and
// extract an integer parameter value.
func GetInt64Value(args map[string]string, name string, def int64) int64 {
	input, ok := args[name]
	if !ok {
		return def
	}
	var mult int64 = 1
	l := len(input)
	if l > 0 {
		if input[l-1] == 'G' || input[l-1] == 'g' {
			input = input[0 : l-1]
			mult = 1024 * 1024 * 1024
		} else if input[l-1] == 'T' || input[l-1] == 't' {
			input = input[0 : l-1]
			mult = 1024 * 1024 * 1024 * 1024
		} else if input[l-1] == 'M' || input[l-1] == 'm' {
			input = input[0 : l-1]
			mult = 1024 * 1024
		} else if input[l-1] == 'K' || input[l-1] == 'k' {
			input = input[0 : l-1]
			mult = 1024
		}
	}
	i, e := strconv.ParseInt(input, 10, 64)
	if e != nil {
		fmt.Printf("Could not parse %s argument to number: %s, error: %v, taking default %d\n", name, input, e, def)
	}
	return i * mult
}

// GetStringValue is used to work on parsed user input from the program and
// extract a string parameter value.
func GetStringValue(args map[string]string, name string, def string) string {
	input, ok := args[name]
	if !ok {
		return def
	}
	return input
}

// GetBoolValue is used to work on parsed user input from the program and
// extract a string parameter value.
func GetBoolValue(args map[string]string, name string, def bool) bool {
	input, ok := args[name]
	if !ok {
		return def
	}
	return len(input) > 0 && (input[0] == 't' || input[0] == 'T' ||
		input[0] == '1' || input[0] == 'y' || input[0] == 'Y')
}

// ParseArguments parses an argument list []string and returns a
// map[string]string. If an argument contains no = sign, it is considered
// to be a flag which is set to `true`. Otherwise the first = sign splits
// the argument into key and value part. Spaces are trimmed on both the key
// and the value. If keys repeat, only the last counts. The first argument
// is special, it is returned as first result if it does not contain an =
// sign. Otherwise, the first result is empty.
func ParseArguments(args []string) (string, map[string]string) {
	var subCmd string
	res := make(map[string]string, 20)
	for i, s := range args {
		ss := strings.TrimSpace(s)
		if len(ss) != 0 { // ignore empty or white only
			pos := strings.Index(ss, "=")
			if pos == -1 {
				if i == 0 {
					subCmd = ss
				} else {
					res[ss] = "true"
				}
			} else {
				key := strings.TrimSpace(ss[0:pos])
				value := strings.TrimSpace(ss[pos+1:])
				res[key] = value
			}
		}
	}
	return subCmd, res
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
	fmt.Printf("%s:\n  %s (median),\n  %s (90%%ile),\n  %s (99%%ilie),\n  %s (average)\n",
		msg,
		times[int(float64(0.5)*float64(nr))],
		times[int(float64(0.9)*float64(nr))],
		times[int(float64(0.99)*float64(nr))],
		time.Duration(sum/nr))
	config.OutputMutex.Unlock()
}
