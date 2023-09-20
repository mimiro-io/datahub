package server

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

type InstrumentedTransaction struct {
	txn    *badger.Txn
	logger *zap.SugaredLogger
	store  *Store
}

type InstumentedIterator struct {
	iter      *badger.Iterator
	options   badger.IteratorOptions
	txn       *InstrumentedTransaction
	itemCalls atomic.Int64
	nextCalls atomic.Int64
}

func (i *InstumentedIterator) Close() {
	i.slowLog(i.iter.Close)
}

func (i *InstumentedIterator) Rewind() {
	i.slowLog(i.iter.Rewind)
}

func (i *InstumentedIterator) Item() *badger.Item {
	defer i.itemCalls.Add(1)
	return slowLogAndReturn(func() *badger.Item { return i.iter.Item() }, i)
}

func (i *InstumentedIterator) Seek(buffer []byte) {
	slowLogAndReturn(func() any { i.iter.Seek(buffer); return nil }, i)
}

func (t *InstrumentedTransaction) NewIterator(options badger.IteratorOptions) *InstumentedIterator {
	it := &InstumentedIterator{t.txn.NewIterator(options), options, t, atomic.Int64{}, atomic.Int64{}}
	return slowLogAndReturn(func() *InstumentedIterator {
		return it
	}, it)
}

func (t *InstrumentedTransaction) Get(id []byte) (*badger.Item, error) {
	return t.txn.Get(id)
}

func (i *InstumentedIterator) ValidForPrefix(bytes []byte) bool {
	return slowLogAndReturn(func() bool { return i.iter.ValidForPrefix(bytes) }, i)
}

func (i *InstumentedIterator) Next() {
	defer i.nextCalls.Add(1)
	i.slowLog(i.iter.Next)
}

func InstrumentedTxn(btxn *badger.Txn, store *Store) *InstrumentedTransaction {
	return &InstrumentedTransaction{txn: btxn, logger: store.logger, store: store}
}

// //////////////////// instrumentation details  /////////////////////////
func (i *InstumentedIterator) slowLog(c func()) {
	t := time.Now()
	c()
	elapsed := time.Since(t)
	if elapsed > i.txn.store.SlowLogThreshold {
		// log slow call
		shortName, structLogPairs := callInfo(i, reflect.ValueOf(c).Pointer())
		structLogPairs = append(structLogPairs, "itemCalls", i.itemCalls.Load())
		structLogPairs = append(structLogPairs, "nextCalls", i.nextCalls.Load())
		i.txn.logger.Infow(fmt.Sprintf("slow badger call: %v , elapsed: %v", shortName, elapsed), structLogPairs...)
	}
}

func slowLogAndReturn[T any](c func() T, i Instrumented) T {
	t := time.Now()
	result := c()
	elapsed := time.Since(t)
	if elapsed > i.getStore().SlowLogThreshold {
		// log slow call
		fptr, _, _, _ := runtime.Caller(1)
		shortName, structLogPairs := callInfo(i, fptr)
		i.getStore().logger.Infow(fmt.Sprintf("slow badger call: %v , elapsed: %v", shortName, elapsed), structLogPairs...)
	}
	return result
}

type Instrumented interface {
	callParams() string
	getStore() *Store
}

func (i *InstumentedIterator) getStore() *Store {
	return i.txn.store
}

func callInfo(i Instrumented, cptr uintptr) (string, []any) {
	f := runtime.FuncForPC(cptr)
	fName := f.Name()
	tokens := strings.Split(fName, ".")
	shortName := tokens[len(tokens)-1]
	if len(tokens) > 1 {
		shortName = tokens[len(tokens)-2] + "." + tokens[len(tokens)-1]
	}

	_, file, line, _ := runtime.Caller(3)

	return shortName, []any{"function", fName, "params", i.callParams(), "file", file, "line", line}
}

func (i *InstumentedIterator) callParams() string {
	return fmt.Sprintf("[prefixDetais:%v, options:%+v ]", i.prefixDetails(i.options.Prefix), i.options)
}

func (i *InstumentedIterator) prefixDetails(prefix []byte) string {
	res := "{"
	if len(prefix) > 0 {
		res += fmt.Sprintf("collection: %v,", collectionToStr(binary.BigEndian.Uint16(prefix)))
	}
	if len(prefix) > 2 {
		internalDsId := binary.BigEndian.Uint32(prefix[2:])
		i.txn.store.datasets.Range(func(_ interface{}, v interface{}) bool {
			ds := v.(*Dataset)
			if ds.InternalID == internalDsId {
				res += fmt.Sprintf("dataset: %v,", ds.ID)
				return false
			}
			return true
		})
	}
	res += "}"
	return res
}