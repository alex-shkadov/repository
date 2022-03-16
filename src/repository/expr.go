package repository

import (
	"strconv"
	"strings"
)

type IN struct {
	Ids      []int64
	OrIsNull bool
}

func (f *IN) AddValue(value interface{}) {
	f.Ids = append(f.Ids, value.(int64))
}

func (f *IN) GetOrIsNull() bool {
	return f.OrIsNull
}

func (f *IN) ToString() string {
	strs := []string{}
	for _, i := range f.Ids {
		strs = append(strs, strconv.Itoa(int(i)))
	}

	return strings.Join(strs, ", ")
}
