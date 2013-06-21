SHELL := /bin/bash

TESTS = $(shell find test -name "*Test.js")
PROJECT = "Engine PointFuel Server"
MOCHA_OPTS = --ignore-leaks
REPORTER = spec

test: 
	@NODE_ENV=test ./node_modules/.bin/mocha $(TESTS)

test-w:
  	@NODE_ENV=test ./node_modules/.bin/mocha $(TESTS) \
    --reporter min \
    --watch \

.PHONY: test test-w