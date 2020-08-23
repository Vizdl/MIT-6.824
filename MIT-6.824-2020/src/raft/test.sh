#!/bin/bash
i=1
until (($?))
do
  rm log.txt
  go test -run TestReElection2A -tags "debug" > log.txt
  echo $i
  let i++
done