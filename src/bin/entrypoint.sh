#!/usr/bin/env bash

set -e

PROJECT=${1:-}

if [ "${PROJECT}" == "analyze-vacuum" ]; then bin/run-analyze-vacuum-utility.sh
elif [ "${PROJECT}" == "column-encoding" ]; then bin/run-column-encoding-utility.sh
elif [ "${PROJECT}" == "unload-copy" ]; then bin/run-unload-copy-utility.sh
elif [ "${PROJECT}" == "user-last-login" ]; then bin/run-user-last-login.sh
elif [ "${PROJECT}" == "admin-views" ]; then bin/run-deploy-admin-views.sh
else echo "Unhandled arg for project to run. Please select from either 'analyze-vacuum', 'column-encoding' or 'unload-copy'"
fi