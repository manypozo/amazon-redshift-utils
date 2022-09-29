#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import logging.config
from os import listdir
from os.path import isfile, join

import boto3

# init logging system at the beginning of your app
LOGGING_CONFIG: dict = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {
            "format": "%(asctime)s - %(levelname)s - %(message)s",
            'datefmt': "%Y-%m-%dT%H:%M:%S",
            "()": "pythonjsonlogger.jsonlogger.JsonFormatter"
        }
    },
    "handlers": {"json": {"class": "logging.StreamHandler", "formatter": "json"}},
    "loggers": {"": {"handlers": ["json"], "level": 20}},
}

logging.config.dictConfig(LOGGING_CONFIG)


def execute_statement(
        stm_name: str,
        sql: str,
        cluster_type: str,
        cluster_id_or_workgroup_name: str,
        db_name: str,
        secret_arn: str) -> dict:
    client = boto3.client("redshift-data")

    if cluster_type.upper() == "SERVERLESS":
        return client.execute_statement(
            WorkgroupName=cluster_id_or_workgroup_name,
            Database=db_name,
            SecretArn=secret_arn,
            Sql=sql,
            StatementName=stm_name,
        )
    else:
        return client.execute_statement(
            ClusterIdentifier=cluster_id_or_workgroup_name,
            Database=db_name,
            SecretArn=secret_arn,
            Sql=sql,
            StatementName=stm_name,
        )


def build_parser():
    parser = argparse.ArgumentParser(description='AdminView deployment')

    parser.add_argument(
        '--sql_path', dest='sql_path', type=str, help='SQL Path'
    )

    parser.add_argument(
        '--cluster_type', choices=['PROVISIONED', 'SERVERLESS', 'provisioned', 'serverless'],
        required=False,
        default='PROVISIONED',
        help=(
            'Redshift cluster type. '
            'If PROVISIONED, connection args are: --cluster_id'
            'If SERVERLESS, connection args are: --workgroup_name'
        )
    )

    parser.add_argument(
        '--cluster_id', dest='cluster_id', type=str, help='ID of the Redshift provisioned cluster to connect to'
    )

    parser.add_argument(
        '--workgroup_name', dest='workgroup_name', type=str, help='Workgroup name to use for the serverless namespace'
    )

    parser.add_argument(
        '--db_name', required=True, dest='db_name', type=str, help='DB name'
    )

    parser.add_argument(
        '--secret_arn', required=True, dest='secret_arn', type=str,
        help='Secret ARN with user-password in key-value format'
    )

    parser.add_argument(
        '--debug', action="store_true",
        required=False,
        default=False,
        dest='debug',
        help='Activate debug mode'
    )

    return parser


def main():
    args = build_parser().parse_args()
    logging.info('Arguments', extra=args.__dict__)

    if args.cluster_type == "SERVERLESS":
        logging.info(f"Deploy AdminView into redshift-serverless {args.workgroup_name}")
    else:
        logging.info(f"Deploy AdminView into redshift-provisioned {args.cluster_id}")

    for f in sorted(listdir(args.sql_path)):
        if isfile(join(args.sql_path, f)) and f.endswith(".sql"):
            with open(join(args.sql_path, f)) as fread:
                sql: str = fread.read()
                index: int = sql.lower().index("create or replace view")
                sql: str = sql[index:]

                if sql.endswith(";"):
                    sql = sql + ";"

                if args.debug:
                    logging.info(f)
                    logging.info(sql)

                else:
                    redshift_id: str = args.cluster_id if args.cluster_type == "SERVERLESS" else args.workgroup_name
                    logging.info(f"Deploying {f}")
                    response = execute_statement(
                        stm_name=f.replace(".sql", ""),
                        sql=sql,
                        cluster_type=args.cluster_type,
                        cluster_id_or_workgroup_name=redshift_id,
                        db_name=args.db_name,
                        secret_arn=args.secret_arn,
                    )
                    logging.info(response)
                    logging.info(f"Done")


if __name__ == "__main__":
    main()
