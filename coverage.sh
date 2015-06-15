#!/bin/bash

echo "mode: set" > acc.out
fail=0

# Standard go tooling behavior is to ignore dirs with leading underscors
for dir in $(find . -maxdepth 10 -not -path './.git*' -not -path '*/_*' -type d);
do
  if ls $dir/*.go &> /dev/null; then
    go test -coverprofile=cover.out $dir || fail=1
    if [ -f cover.out ]
    then
      cat cover.out | grep -v "mode: set" >> acc.out
      rm cover.out
    fi
  fi
done

exit $fail
