package boltsharp

import (
	"fmt"
	"github.com/matsuwin/syscat/cat"
	"regexp"
	"strings"
)

type QueryRulesNode struct {
	Condition string            `json:"condition,omitempty"`
	Depth     int               `json:"depth,omitempty"` // 深度
	Rules     []*QueryRulesNode `json:"rules,omitempty"`
	Field     string            `json:"field,omitempty"`
	Value     interface{}       `json:"value,omitempty"`
	Operator  string            `json:"operator,omitempty"`
}

// 条件匹配状态机
func matchingStateMachine(debug bool, index []byte, rules *QueryRulesNode) (state bool) {
	if rules == nil {
		return false
	}

	if debug && rules.Condition != "" {
		fmt.Printf("%s| %s\n", strings.Repeat("——", rules.Depth), rules.Condition)
	}

	// 条件分支
	switch rules.Condition {
	case QueryConditionAND:
		for i := 0; i < len(rules.Rules); i++ {
			state = matchingStateMachine(debug, index, rules.Rules[i])
			if !state {
				return false
			}
		}
	case QueryConditionOR:
		for i := 0; i < len(rules.Rules); i++ {
			state = matchingStateMachine(debug, index, rules.Rules[i])
			if state {
				return true
			}
		}
	}

	// 条件适配
	if rules.Field != "" {
		state = stateOperator(index, rules)
		if debug {
			fmt.Printf("%s| %s %s %#v : %v\n", strings.Repeat("——", rules.Depth), rules.Field, rules.Operator, rules.Value, state)
		}
	}

	return state
}

func stateOperator(index []byte, rules *QueryRulesNode) (state bool) {
	switch rules.Operator {
	case QueryOperatorEquals, QueryOperatorNotEquals:
		re := regexp.MustCompile(fmt.Sprintf(";'%s'%s;", rules.Field, rules.Value))
		state = re.Match(index)
		if rules.Operator == QueryOperatorNotEquals {
			state = !state
		}
	case QueryOperatorContains:
		seed := strings.Join([]string{";'" + rules.Field + "'"}, "")
		value := cat.BytesToString(regexp.MustCompile(seed + ".+;'").Find(index))
		if len(value) > len(seed) {
			state = strings.Contains(value[len(seed):], fmt.Sprintf("%s", rules.Value))
		}
	}
	return state
}

func NewQueryIndexes(unixMilli string, fields []string) string {
	var indexes, key string
	for i := 0; i < len(fields); i++ {
		if key == "" {
			key = fields[i]
		} else {
			if fields[i] != "" {
				indexes += fmt.Sprintf("'%s'%s;", key, fields[i])
			}
			key = ""
		}
	}
	return unixMilli + ";" + indexes
}

const (
	QueryConditionAND      = "AND"
	QueryConditionOR       = "OR"
	QueryOperatorEquals    = "=="
	QueryOperatorNotEquals = "!="
	QueryOperatorContains  = "CONTAINS"
)
