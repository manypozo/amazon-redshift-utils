#!/usr/bin/env bash

set -e

echo "Deploying AdminViews"

# Required
AWS_REGION=${AWS_REGION:-}
CLUSTER_IDENTIFIER=${CLUSTER_IDENTIFIER:-""}
WORKGROUP_NAME=${WORKGROUP_NAME:-""}
DB_NAME=${DB_NAME:-}
SECRET_ARN=${SECRET_ARN:-}

# Optional with Defaults
IS_SERVERLESS=${IS_SERVERLESS:-false}

if [ "${SECRET_ARN}" == "" ]; then echo "Environment Var 'SECRET_ARN' must be defined"; exit 1
elif [ "${AWS_REGION}" == "" ]; then echo "Environment Var 'AWS_REGION' must be defined"
else
    if [ "${IS_SERVERLESS}" == "false" ]; then
      echo "Target is the redshift-provisioned '${CLUSTER_IDENTIFIER}'"
      python3 AdminViews/run_deply_admin_views.py \
        --cluster_type=provisioned
        --cluster_id=${CLUSTER_IDENTIFIER}
        --db_name=${DB_NAME}
        --secret_arn=${SECRET_ARN}
        --sql_path="./AdminViews"
    else
      echo "Target is the redshift-serverless '${WORKGROUP_NAME}'"
      python3 AdminViews/run_deply_admin_views.py \
        --cluster_type=serverless
        --workgroup_name=${WORKGROUP_NAME}
        --db_name=${DB_NAME}
        --secret_arn=${SECRET_ARN}
        --sql_path="./AdminViews"
    fi
fi
