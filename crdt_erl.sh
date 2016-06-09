#!/bin/bash

cd /Users/gyanendraaggarwal/erlang/code/erlang_crdt

erl -sname $1 -pa ./ebin -config ./sys
