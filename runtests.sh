#! /bin/bash
set -e

if [ -z "$TEST_DATABASE_URL" ]
then
        dropdb toolkit_test --if-exists
        createdb toolkit_test
        export TEST_DATABASE_URL=postgres:///toolkit_test
fi
pytest --cov=toolkit --flakes "$@"