package boltsharp

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

/*
——| AND
————| OR
——————| category == "/DDOS" : true
——————| attacker CONTAINS "test" : true
————| direction != "01" : true
result: true
*/

func Test(t *testing.T) {
	unixMilli := fmt.Sprintf("%d", time.Now().UnixMilli())
	index := NewQueryIndexes(unixMilli, []string{
		"direction", "02",
		"attacker", "ab76test8a",
		"category", "/DDOS",
	})
	fmt.Println(index)

	rules := &QueryRulesNode{}

	if err := json.Unmarshal([]byte(testDATA), rules); err != nil {
		panic(err)
	}
	result := matchingStateMachine(true, []byte(index), rules)
	fmt.Printf("result: %v\n", result)
}

const testDATA = `
{
   "condition": "AND",
   "depth":1,
   "rules": [
      {
         "condition": "OR",
         "depth":2,
         "rules": [
            {
               "depth":3,
               "field": "category",
               "value": "/DDOS",
               "operator": "=="
            },
            {
               "depth":3,
               "field": "attacker",
               "value": "test",
               "operator": "CONTAINS"
            }
         ]
      },
      {
         "depth":2,
         "field": "direction",
         "value": "01",
         "operator": "!="
      }
   ]
}
`
