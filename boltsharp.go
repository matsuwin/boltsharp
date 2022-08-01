package boltsharp

import (
	"bytes"
	"fmt"
	"github.com/matsuwin/syscat/cat"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

// 数据库句柄恢复中检测，三秒后任未恢复则抛出错误。
func recovering(db *bbolt.DB) error {
	i := 0
re:
	if db == nil || db.Path() == "" {
		if i < 3 {
			return errors.New("Database handle Recovery failed!")
		}
		i++
		time.Sleep(time.Second)
		goto re
	}
	return nil
}

func fetchRules(rules ...interface{}) (node *QueryRulesNode, match []string) {
	if len(rules) == 0 {
		return
	}
	if rules[0] == nil {
		return
	}
	vType := fmt.Sprintf("%T", rules[0])
	switch vType {
	case "string":
		ss := make([]string, 0, len(rules))
		for i := 0; i < len(rules); i++ {
			if rules[i] == nil {
				continue
			}
			ss = append(ss, rules[i].(string))
		}
		return nil, ss
	case "*boltsharp.QueryRulesNode":
		return rules[0].(*QueryRulesNode), nil
	default:
		panic(errors.New("Unsupported types '" + vType + "'"))
	}
}

// Select 数据检索，支持时间范围和正则扫描
// @param min    起始时间（毫秒）
// @param max    结束时间（毫秒）
// @param match  正则语句，对 KEY 产生作用
// @param sort   落盘顺序排序，正序(1) 倒序(-1)
// @param limit  数据长度
// @param rules  查询条件
func Select(db *bbolt.DB, first, last int64, sort, limit int, rules ...interface{}) ([]*Element, error) {
	if err := recovering(db); err != nil {
		return nil, err
	}
	node, match := fetchRules(rules...)

	// 查询初始化
	var regs = make([]*regexp.Regexp, 0, len(match))
	for i := 0; i < len(match); i++ {
		regs = append(regs, regexp.MustCompile(match[i]))
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	start, finish := tfl2bytes(first, last)
	data := make([]*Element, 0, 100)

	// 装载模块
	matching := func(k, v []byte) int8 {
		for i := 0; i < len(regs); i++ {
			if !regs[i].Match(k) {
				return 1
			}
			// fmt.Printf("%s  <%v>\n", k, match)
		}
		if len(data) >= limit {
			return 2
		}
		if node != nil {
			if !matchingStateMachine(false, k, node) {
				return 1
			}
		}
		data = append(data, &Element{cat.BytesToString(k), v})
		return 0
	}

	// 检索模块
	view := func(tx *bbolt.Tx) error {
		cur := tx.Bucket(bucket).Cursor()
		if sort == 1 {
			for k, v := cur.Seek(start); k != nil && bytes.Compare(k, finish) <= 0; k, v = cur.Next() {
				switch matching(k, v) {
				case 1:
					continue
				case 2:
					break
				}
			}
		} else if sort == -1 {
			k, v := cur.Last()
			if len(k) > 13 {
				if t := k[:13]; cat.BytesToString(k) < cat.BytesToString(finish) {
					finish = t
				}
			}
			for k, v = cur.Seek(finish); k != nil; k, v = cur.Prev() {
				if bytes.Compare(k, start) <= 0 {
					continue
				}
				switch matching(k, v) {
				case 1:
					continue
				case 2:
					break
				}
			}
		}
		return nil
	}

	// 开始查询
	if err := db.View(view); err != nil {
		return nil, errors.New(err.Error())
	}
	return data, nil
}

// SetAll 批量写入数据，value != nil ? insert : delete
func SetAll(db *bbolt.DB, elements []*Element) error {
	if err := recovering(db); err != nil {
		return err
	}
	update := func(tx *bbolt.Tx) (err error) {
		b := tx.Bucket(bucket)
		for i := 0; i < len(elements); i++ {
			if elements[i] == nil {
				continue
			}
			if elements[i].Value != nil {
				err = b.Put(cat.StringToBytes(&elements[i].Index), elements[i].Value)
			} else {
				err = b.Delete(cat.StringToBytes(&elements[i].Index))
			}
			if err != nil {
				return
			}
		}
		return
	}
	if err := db.Update(update); err != nil {
		return errors.New(err.Error())
	}
	return nil
}

// CleanExpiredData 清理过期的数据，可以实现周期内的数据滚动存储。
func CleanExpiredData(db *bbolt.DB, d time.Duration) (int, error) {
	if err := recovering(db); err != nil {
		return 0, err
	}
	t := time.Now()
	elems, err := Select(db, 0, t.Add(-d).UnixMilli(), 1, 0)
	if err != nil {
		return 0, err
	}
	for i := 0; i < len(elems); i++ {
		if elems[i] == nil {
			continue
		}
		elems[i].Value = nil
	}
	if err = SetAll(db, elems); err != nil {
		return 0, err
	}
	return len(elems), nil
}

// New 打开数据库
func New(dbname string) *bbolt.DB {
	_, readErr := os.Stat(dbname)
	_ = os.MkdirAll(filepath.Dir(dbname), 0777)
	db, err := bbolt.Open(dbname, 0666, nil)
	if err != nil {
		if err.Error() == "invalid database" {
			os.Remove(dbname)
		}
		panic(errors.New(err.Error()))
	}
	if readErr != nil {
		update := func(tx *bbolt.Tx) error {
			b, createErr := tx.CreateBucketIfNotExists(bucket)
			if createErr != nil {
				return createErr
			}
			return b.Put([]byte("test"), []byte("test"))
		}
		if err = db.Update(update); err != nil {
			panic(errors.New(err.Error()))
		}
		_ = SetAll(db, ValuesByMap(map[string][]byte{"test": nil}))
	}
	return db
}

type Element struct {
	Index string
	Value []byte
}

var bucket = []byte("bucket")

func ValuesByMap(data map[string][]byte) []*Element {
	elements := make([]*Element, 0, len(data))
	for index, value := range data {
		elements = append(elements, &Element{index, value})
	}
	return elements
}

func Get(db *bbolt.DB, index string) (value []byte, _ error) {
	if err := recovering(db); err != nil {
		return nil, err
	}
	view := func(tx *bbolt.Tx) error {
		value = tx.Bucket(bucket).Get(cat.StringToBytes(&index))
		return nil
	}
	if err := db.View(view); err != nil {
		return nil, errors.New(err.Error())
	}
	return
}

func GetKeyAll(db *bbolt.DB) ([]string, error) {
	if err := recovering(db); err != nil {
		return nil, err
	}
	keys := make([]string, 0, 100)
	view := func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		return b.ForEach(func(k, v []byte) error {
			keys = append(keys, cat.BytesToString(k))
			return nil
		})
	}
	if err := db.View(view); err != nil {
		return nil, errors.New(err.Error())
	}
	return keys, nil
}

func DeleteAll(db *bbolt.DB, keys []string) error {
	if err := recovering(db); err != nil {
		return err
	}
	update := func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucket)
		for i := 0; i < len(keys); i++ {
			err := b.Delete(cat.StringToBytes(&keys[i]))
			if err != nil {
				return err
			}
		}
		return nil
	}
	if err := db.Update(update); err != nil {
		return errors.New(err.Error())
	}
	return nil
}

func tfl2bytes(first, last int64) (_, _ []byte) {
	First := fmt.Sprintf("%d", first)
	Last := fmt.Sprintf("%d", last)
	return cat.StringToBytes(&First), cat.StringToBytes(&Last)
}
