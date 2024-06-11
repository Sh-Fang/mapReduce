package mapreduce

import (
	"fmt"
	"strings"
	"sync"
)

// KeyValue 结构体
type KeyValue struct {
	Key   string
	Value string
}

// 定义 Map 和 Reduce 函数类型
type MapFunc func(string, string) []KeyValue
type ReduceFunc func(string, []string) string

// Map 函数：统计单词出现次数
func mapFunc(filename string, contents string) []KeyValue {
	var kva []KeyValue
	words := strings.Fields(contents)
	for _, w := range words {
		kva = append(kva, KeyValue{w, "1"})
	}
	return kva
}

// Reduce 函数：合并相同单词的计数
func reduceFunc(key string, values []string) string {
	count := 0
	for _, v := range values {
		count += 1
	}
	return fmt.Sprintf("%d", count)
}

// Master 结构体
type Master struct {
	mapF         MapFunc
	reduceF      ReduceFunc
	files        map[string]string
	intermediate []KeyValue
	mu           sync.Mutex
	wg           sync.WaitGroup
}

// 创建新的 Master
func NewMaster(files map[string]string, mapF MapFunc, reduceF ReduceFunc) *Master {
	return &Master{
		mapF:    mapF,
		reduceF: reduceF,
		files:   files,
	}
}

// 执行 MapReduce
func (m *Master) Run() map[string]string {
	// 执行 Map 阶段
	for filename, contents := range m.files {
		m.wg.Add(1)
		go func(filename string, contents string) {
			defer m.wg.Done()
			kva := m.mapF(filename, contents)
			m.mu.Lock()
			m.intermediate = append(m.intermediate, kva...)
			m.mu.Unlock()
		}(filename, contents)
	}
	m.wg.Wait()

	// 按 key 对中间结果进行分组
	groups := make(map[string][]string)
	for _, kv := range m.intermediate {
		groups[kv.Key] = append(groups[kv.Key], kv.Value)
	}

	// 执行 Reduce 阶段
	result := make(map[string]string)
	for key, values := range groups {
		result[key] = m.reduceF(key, values)
	}

	return result
}

func main() {
	// 输入文件
	files := map[string]string{
		"file1.txt": "hello world",
		"file2.txt": "hello go",
	}

	// 创建 Master 并执行 MapReduce
	master := NewMaster(files, mapFunc, reduceFunc)
	result := master.Run()

	// 输出结果
	for key, value := range result {
		fmt.Printf("%s: %s\n", key, value)
	}
}
