package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

const (
	SET      = "SET"
	GET      = "GET"
	DELETE   = "DELETE"
	COUNT    = "COUNT"
	BEGIN    = "BEGIN"
	ROLLBACK = "ROLLBACK"
	COMMIT   = "COMMIT"
)

type (
	Command struct {
		Typ   string
		Key   string
		Value string
	}

	Storage struct {
		mutex     sync.RWMutex
		data      map[string]string
		rollbacks [][]Command
	}
)

func NewStorage() *Storage {
	return &Storage{
		data:      map[string]string{},
		rollbacks: [][]Command{},
	}
}

func (s *Storage) ProcessCommand(cmd Command) {
	switch cmd.Typ {
	case SET:
		s.set(cmd.Key, cmd.Value)
	case GET:
		fmt.Println(s.get(cmd.Key))
	case DELETE:
		s.delete(cmd.Key)
	case COUNT:
		fmt.Println(s.count(cmd.Key))
	case BEGIN:
		s.begin()
	case ROLLBACK:
		s.rollback()
	case COMMIT:
		s.commit()
	}
}

func (s *Storage) set(key, value string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(s.rollbacks) > 0 {
		var cmd Command

		lastValue, ok := s.data[key]
		if !ok {
			cmd = Command{
				Typ: DELETE,
				Key: key,
			}
		} else {
			cmd = Command{
				Typ:   SET,
				Key:   key,
				Value: lastValue,
			}
		}

		lastTransactionRollbacks := s.rollbacks[len(s.rollbacks)-1]
		lastTransactionRollbacks = append(lastTransactionRollbacks, cmd)
		s.rollbacks[len(s.rollbacks)-1] = lastTransactionRollbacks
	}

	s.data[key] = value
}

func (s *Storage) get(key string) string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	value, ok := s.data[key]
	if !ok {
		return "NULL"
	}

	return value
}

func (s *Storage) delete(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(s.rollbacks) > 0 {
		lastValue, ok := s.data[key]
		if ok {
			lastTransactionRollbacks := s.rollbacks[len(s.rollbacks)-1]
			lastTransactionRollbacks = append(lastTransactionRollbacks, Command{
				Typ:   SET,
				Key:   key,
				Value: lastValue,
			})
			s.rollbacks[len(s.rollbacks)-1] = lastTransactionRollbacks
		}
	}

	delete(s.data, key)
}

func (s *Storage) count(value string) int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	count := 0

	for _, v := range s.data {
		if value == v {
			count++
		}
	}

	return count
}

func (s *Storage) begin() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.rollbacks = append(s.rollbacks, []Command{})
}

func (s *Storage) rollback() {
	if !(len(s.rollbacks) > 0) {
		fmt.Println("NO TRANSACTION")
		return
	}

	s.mutex.RLock()
	lastTransactionRollbacks := s.rollbacks[len(s.rollbacks)-1]
	s.mutex.RUnlock()

	for _, cmd := range lastTransactionRollbacks {
		s.ProcessCommand(cmd)
	}

	s.mutex.Lock()
	s.rollbacks = s.rollbacks[:len(s.rollbacks)-1]
	s.mutex.Unlock()
}

func (s *Storage) commit() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.rollbacks = [][]Command{}
}

func main() {
	storage := NewStorage()
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Printf(">>> ")

	for scanner.Scan() {
		line := scanner.Text()
		worlds := strings.Split(line, " ")

		cmd := Command{
			Typ: worlds[0],
		}

		if len(worlds) > 1 {
			cmd.Key = worlds[1]
		}

		if len(worlds) > 2 {
			cmd.Value = worlds[2]
		}

		storage.ProcessCommand(cmd)

		fmt.Printf(">>> ")
	}
}
