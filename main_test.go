package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleOperations(t *testing.T) {
	storage := NewStorage()

	storage.set("a", "10")
	assert.Equal(t, "10", storage.get("a"))

	storage.set("b", "10")
	storage.set("c", "20")
	assert.Equal(t, 2, storage.count("10"))

	storage.delete("b")
	assert.Equal(t, "NULL", storage.get("b"))

	assert.Equal(t, 1, storage.count("10"))
}

func TestTransaction(t *testing.T) {
	storage := NewStorage()

	storage.set("a", "10")
	assert.Equal(t, "10", storage.get("a"))
	storage.set("b", "10")
	assert.Equal(t, "10", storage.get("b"))

	storage.begin()
	storage.set("a", "20")
	storage.delete("b")
	assert.Equal(t, "20", storage.get("a"))
	assert.Equal(t, "NULL", storage.get("b"))
	storage.rollback()

	assert.Equal(t, "10", storage.get("a"))
	assert.Equal(t, "10", storage.get("b"))

	storage.begin()
	storage.set("a", "20")
	storage.delete("b")
	assert.Equal(t, "20", storage.get("a"))
	assert.Equal(t, "NULL", storage.get("b"))
	storage.commit()

	assert.Equal(t, "20", storage.get("a"))
	assert.Equal(t, "NULL", storage.get("b"))
}

func TestNestedTransaction(t *testing.T) {
	storage := NewStorage()

	storage.set("a", "10")
	assert.Equal(t, "10", storage.get("a"))

	storage.begin()
	storage.set("a", "20")
	assert.Equal(t, "20", storage.get("a"))

	storage.begin()
	storage.set("a", "30")
	assert.Equal(t, "30", storage.get("a"))
	storage.rollback()

	assert.Equal(t, "20", storage.get("a"))

	storage.rollback()
	assert.Equal(t, "10", storage.get("a"))

	storage.begin()
	storage.set("a", "20")
	assert.Equal(t, "20", storage.get("a"))

	storage.begin()
	storage.set("a", "30")
	assert.Equal(t, "30", storage.get("a"))

	storage.commit()

	assert.Equal(t, "30", storage.get("a"))
}
