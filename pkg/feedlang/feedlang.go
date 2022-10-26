package feedlang

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Program interface {
	Execute() error
}

type Maker func(args []string) (Program, error)

var Atoms map[string]Maker

type Sequential struct {
	Steps []Program
}

type Parallel struct {
	Steps []Program
}

func (s *Sequential) Execute() error {
	for _, step := range s.Steps {
		err := step.Execute()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Parallel) Execute() error {
	nr := len(s.Steps)
	errs := make([]error, nr, nr)
	wg := sync.WaitGroup{}
	for i, step := range s.Steps {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int, p Program) {
			defer wg.Done()
			errs[i] = p.Execute()
		}(&wg, i, step)
	}
	wg.Wait()
	for i := 0; i < nr; i += 1 {
		if errs[i] != nil {
			return errs[i]
		}
	}
	return nil
}

type DummyProg struct {
	i int
}

func (dp *DummyProg) Execute() error {
	fmt.Printf("Dummy: I am working %d\n", dp.i)
	time.Sleep(time.Second * time.Duration(dp.i))
	fmt.Printf("Dummy: I am done %d\n", dp.i)
	return nil
}

func DummyMaker(args []string) (Program, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("Expecting one integer argument!")
	}
	i, err := strconv.ParseInt(args[0], 10, 0)
	if err != nil {
		return nil, fmt.Errorf("Could not parse integer %s: %v\n", args[0], err)
	}
	return &DummyProg{i: int(i)}, nil
}

func init() {
	Atoms = make(map[string]Maker, 1)
	Atoms["dummy"] = DummyMaker
}

func getLine(lines []string, pos *int) (string, error) {
	// Trim line and ignore empty and comment lines:
	var line string
	for *pos < len(lines) {
		line = strings.TrimSpace(lines[*pos])
		if len(line) >= 0 && line[0] != '#' {
			return line, nil
		}
		*pos += 1
	}
	return "", fmt.Errorf("Unexpected end of input in line %d", *pos+1)
}

// parserRecursion parses one Program starting at pos in lines and
// returns it if this is possible. pos is advanced.
func parserRecursion(lines []string, pos *int, depth int) (Program, error) {
	line, err := getLine(lines, pos)
	if err != nil {
		return nil, err
	}
	if line == "{" {
		// Open new parallel subprogram
		*pos += 1
		var par Parallel
		par.Steps = make([]Program, 0, 16)
		for {
			line, err := getLine(lines, pos)
			if err != nil {
				return nil, err
			}
			if line == "}" {
				break
			}
			prog, err := parserRecursion(lines, pos, depth+1)
			if err != nil {
				return nil, err
			}
			par.Steps = append(par.Steps, prog)
		}
		*pos += 1 // consume ]
		return &par, nil
	} else if line == "[" {
		// Open new sequential subprogram
		*pos += 1
		var seq Sequential
		seq.Steps = make([]Program, 0, 16)
		for {
			line, err := getLine(lines, pos)
			if err != nil {
				return nil, err
			}
			if line == "]" {
				break
			}
			prog, err := parserRecursion(lines, pos, depth+1)
			if err != nil {
				return nil, err
			}
			seq.Steps = append(seq.Steps, prog)
		}
		*pos += 1 // consume ]
		return &seq, nil
	} else if line == "}" {
		return nil, fmt.Errorf("Unexpected '}' in line %d of input in depth %d", *pos+1, depth)
	} else if line == "]" {
		return nil, fmt.Errorf("Unexpected ']' in line %d of input in depth %d", *pos+1, depth)
	}
	args := strings.Split(strings.TrimSpace(lines[*pos]), " ")
	if len(args) == 0 {
		return nil, fmt.Errorf("Unexpected empty line in line %d of input in depth %d", *pos+1, depth)
	}
	cmd := args[0]
	args = args[1:]
	maker, ok := Atoms[cmd]
	if !ok {
		return nil, fmt.Errorf("No Maker for first word %s in line %d of input in depth %d", cmd, *pos+1, depth)
	}
	prog, err := maker(args)
	if err != nil {
		return nil, fmt.Errorf("Got error from Maker for %s with args %v in line %d of input in depth %d", cmd, args, *pos+1, depth)
	}
	*pos += 1
	return prog, nil
}

func Parse(lines []string) (Program, error) {
	var pos int = 0
	prog, err := parserRecursion(lines, &pos, 0)
	if err != nil {
		return nil, err
	}
	if pos < len(lines) {
		return nil, fmt.Errorf("Parsing ended before end of lines in line %d (out of %d lines)", pos+1, len(lines))
	}
	return prog, nil
}
