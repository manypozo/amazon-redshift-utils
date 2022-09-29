#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import logging.config
from typing import List
from urllib import parse

from sqlalchemy import create_engine

from metric_model import MetricModel, DimensionModel
from query_config_model import QueryConfigModel
from query_service import load_queries, gather_table_stats, gather_user_group_stats, gather_service_class_stats, \
    run_custom_query
from redshift_connection import ConnectionDetails, get_connection_details_provisioned, get_connection_details_serverless

__APP_NAME__ = "RedshiftAdvancedMonitoring"
__VERSION__ = "v0.1.0"

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


def build_parser():
    parser = argparse.ArgumentParser(description='Monitoring queries')

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
        '--db_user', required=False, dest='db_user', type=str, help='DB username'
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

    # parse queries
    path: str = args.sql_path
    logging.info(f"Query configuration path={path}")
    queries: List[QueryConfigModel] = load_queries(path=path)

    # get db credentials
    is_serverless: bool = args.cluster_type == "SERVERLESS" or args.cluster_type == "serverless"
    redshift_creds: ConnectionDetails = None
    if is_serverless:
        redshift_creds = get_connection_details_serverless(workgroup_name=args.workgroup_name, db_name=args.db_name)
    else:
        redshift_creds = get_connection_details_provisioned(cluster_id=args.cluster_id, db_name=args.db_name,
                                                            user=args.db_user)

    conn_sqlalchemy = create_engine(
        'postgresql://{user}:{password}@{host}:{port}/{dbname}'.format(
            user=parse.quote(redshift_creds.user),
            password=parse.quote(redshift_creds.password),
            host=redshift_creds.endpoint,
            port=redshift_creds.port,
            dbname=redshift_creds.db_name))

    logging.info("Redshift credentials successfully extracted")

    # main dimensions for metrics
    common_dimensions: List[DimensionModel] = []
    if is_serverless:
        common_dimensions.append(DimensionModel(name="WorkgroupName", value=args.workgroup_name))
    else:
        common_dimensions.append(DimensionModel(name="ClusterIdentifier", value=args.cluster_id))

    # run queries
    logging.info("Run monitoring queries")
    metrics: List[MetricModel] = []
    with conn_sqlalchemy.begin() as conn:
        # set application name
        _sql_set_name = f"SET application_name TO '{__APP_NAME__}-{__VERSION__}'"
        conn.execute(_sql_set_name)

        logging.info(f"Collect table stats")
        new_metrics: List[MetricModel] = gather_table_stats(conn=conn, dimensions=common_dimensions)
        metrics.extend(new_metrics)

        if not is_serverless:
            logging.info(f"Collect service class metrics")
            new_metrics: List[MetricModel] = gather_service_class_stats(conn=conn, dimensions=common_dimensions)
            metrics.extend(new_metrics)

        if not is_serverless:
            logging.info(f"Collect user group metrics")
            new_metrics: List[MetricModel] = gather_user_group_stats(conn=conn, dimensions=common_dimensions)
            metrics.extend(new_metrics)

        # collect custom metrics
        # run the externally configured commands and append their values onto the put metrics
        logging.info(f"Collect custom metrics")
        for q in queries:
            logging.info(f"Running query={q.name} statement={q.get_statement(inline=True)}")
            m: MetricModel = run_custom_query(conn=conn, query=q, dimensions=common_dimensions)
            if m:
                metrics.append(m)

    for m in metrics:
        # rename some metric attributes to avoid reserved logging keywords
        # RESERVED_ATTRS = (
        #     'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
        #     'funcName', 'levelname', 'levelno', 'lineno', 'module',
        #     'msecs', 'message', 'msg', 'name', 'pathname', 'process',
        #     'processName', 'relativeCreated', 'stack_info', 'thread', 'threadName')
        logging.info("metric", extra=m.to_dict())


if __name__ == '__main__':
    main()
