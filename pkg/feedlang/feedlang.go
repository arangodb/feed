package feedlang

import (
	"fmt"
	"github.com/arangodb/feed/pkg/config"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ProgMeta struct {
	StartLine int           `json:"startLine"`
	EndLine   int           `json:"endLine"`
	StartTime time.Time     `json:"startTime"`
	EndTime   time.Time     `json:"endTime"`
	RunTime   time.Duration `json:"runTime"`
	Type      string        `json:"type"`
}

type Program interface {
	Execute() error         // Runs the program
	Lines() (int, int)      // Returns the input lines of the program
	StatsOutput() []string  // Run after Execute
	StatsJSON() interface{} // Run after Execute, result must be serializable
}

type Maker func(args []string, line int) (Program, error)

var Atoms map[string]Maker

type Sequential struct {
	Steps []Program
	ProgMeta
}

func (s *Sequential) Lines() (int, int) {
	return s.StartLine, s.EndLine
}

func (s *Sequential) StatsOutput() []string {
	res := []string{
		fmt.Sprintf("sequential: Runtime: %v (lines %d..%d of script)\n",
			s.EndTime.Sub(s.StartTime), s.StartLine, s.EndLine),
		fmt.Sprintf("      Start time: %v\n", s.StartTime),
		fmt.Sprintf("      End time  : %v\n", s.EndTime),
	}
	for i := 0; i < len(s.Steps); i += 1 {
		res = append(res, []string{
			"\n",
			fmt.Sprintf("Step %d (%d) of sequential program:\n", i+1, len(s.Steps)),
			"\n",
		}...)
		sub := s.Steps[i].StatsOutput()
		res = append(res, sub...)
		res = append(res, "\n")
	}
	res = append(res, fmt.Sprintf("sequential: Report finished (lines %d..%d of scripts)\n",
		s.StartLine, s.EndLine))
	res = append(res, "\n")
	return res
}

type SequentialStats struct {
	Steps []interface{}
	ProgMeta
}

func (s *Sequential) StatsJSON() interface{} {
	ss := SequentialStats{ProgMeta: s.ProgMeta}
	ss.Type = "sequential"
	for _, st := range s.Steps {
		ss.Steps = append(ss.Steps, st.StatsJSON())
	}
	return ss
}

type Parallel struct {
	Steps []Program
	ProgMeta
}

type ParallelStats struct {
	Steps []interface{}
	ProgMeta
}

func (p *Parallel) Lines() (int, int) {
	return p.StartLine, p.EndLine
}

func (p *Parallel) StatsOutput() []string {
	res := []string{
		fmt.Sprintf("parallel: Runtime: %v (lines %d..%d of script)\n",
			p.EndTime.Sub(p.StartTime), p.StartLine, p.EndLine),
		fmt.Sprintf("      Start time: %v\n", p.StartTime),
		fmt.Sprintf("      End time  : %v\n", p.EndTime),
	}
	for i := 0; i < len(p.Steps); i += 1 {
		res = append(res, []string{
			"\n",
			fmt.Sprintf("Step %d (%d) of parallel program:\n", i+1, len(p.Steps)),
			"\n",
		}...)
		sub := p.Steps[i].StatsOutput()
		res = append(res, sub...)
		res = append(res, "\n")
	}
	res = append(res, fmt.Sprintf("sequential: Report finished (lines %d..%d of scripts)\n",
		p.StartLine, p.EndLine))
	res = append(res, "\n")
	return res
}

func (p *Parallel) StatsJSON() interface{} {
	ps := ParallelStats{ProgMeta: p.ProgMeta}
	ps.Type = "parallel"
	for _, st := range p.Steps {
		ps.Steps = append(ps.Steps, st.StatsJSON())
	}
	return ps
}

// Execution:

func (s *Sequential) Execute() error {
	s.StartTime = time.Now()
	for _, step := range s.Steps {
		err := step.Execute()
		if err != nil {
			s.EndTime = time.Now()
			s.RunTime = s.EndTime.Sub(s.StartTime)
			return err
		}
	}
	s.EndTime = time.Now()
	s.RunTime = s.EndTime.Sub(s.StartTime)
	return nil
}

func (s *Parallel) Execute() error {
	s.StartTime = time.Now()
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
	s.EndTime = time.Now()
	s.RunTime = s.EndTime.Sub(s.StartTime)
	for i := 0; i < nr; i += 1 {
		if errs[i] != nil {
			return errs[i]
		}
	}
	return nil
}

type WaitProg struct {
	i int `json:"-"`
	ProgMeta
}

func (dp *WaitProg) Execute() error {
	dp.StartTime = time.Now()
	config.OutputMutex.Lock()
	fmt.Printf("%v: wait: I am working %d (line %d of script)\n", time.Now(), dp.i, dp.StartLine)
	config.OutputMutex.Unlock()

	time.Sleep(time.Second * time.Duration(dp.i))

	config.OutputMutex.Lock()
	fmt.Printf("%v: wait: I am done %d (line %d of script)\n", time.Now(), dp.i, dp.StartLine)
	config.OutputMutex.Unlock()
	dp.EndTime = time.Now()
	dp.RunTime = dp.EndTime.Sub(dp.StartTime)
	return nil
}

func (w *WaitProg) Lines() (int, int) {
	return w.StartLine, w.EndLine
}

func (w *WaitProg) StatsOutput() []string {
	return []string{
		fmt.Sprintf("wait: Have waited %d seconds (lines %d..%d of script)\n",
			w.i, w.StartLine, w.EndLine),
		fmt.Sprintf("      Start time: %v\n", w.StartTime),
		fmt.Sprintf("      End time  : %v\n", w.EndTime),
	}
}

func (w *WaitProg) StatsJSON() interface{} {
	w.Type = "wait"
	return w
}

func WaitMaker(args []string, line int) (Program, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("Expecting one integer argument!")
	}
	i, err := strconv.ParseInt(args[0], 10, 0)
	if err != nil {
		return nil, fmt.Errorf("Could not parse integer %s: %v\n", args[0], err)
	}
	return &WaitProg{i: int(i), ProgMeta: ProgMeta{StartLine: line, EndLine: line}}, nil
}

func init() {
	if Atoms == nil {
		Atoms = make(map[string]Maker, 100)
	}
	Atoms["wait"] = WaitMaker
}

func getLine(lines []string, pos *int) (string, error) {
	// Trim line and ignore empty and comment lines:
	var line string
	for *pos < len(lines) {
		line = strings.TrimSpace(lines[*pos])
		if len(line) > 0 && line[0] != '#' {
			return line, nil
		}
		*pos += 1
	}
	return "", fmt.Errorf("Unexpected end of input in line %d", *pos+1)
}

// parserRecursion parses one Program starting at pos in lines and
// returns it if this is possible. pos is advanced.
func parserRecursion(lines []string, pos *int, depth int) (Program, error) {
	startLine := *pos
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
		par.StartLine = startLine + 1
		par.EndLine = *pos + 1
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
		seq.StartLine = startLine + 1
		seq.EndLine = *pos + 1
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
	prog, err := maker(args, *pos+1)
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
