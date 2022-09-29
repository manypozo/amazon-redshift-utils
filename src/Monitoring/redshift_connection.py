#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging

import boto3
import psycopg2
from psycopg2.extras import LoggingConnection


class ConnectionDetails:

    def __init__(self, endpoint: str, db_name: str, port: int, user: str, password: str):
        self.endpoint = endpoint
        self.db_name = db_name
        self.port = port
        self.user = user
        self.password = password


def get_connection_details_provisioned(cluster_id: str, db_name: str, user: str, group=None):
    cli = boto3.client('redshift')
    kwargs = {}
    if group:
        kwargs['DbGroups'] = [group]
    response = cli.get_cluster_credentials(
        ClusterIdentifier=cluster_id,
        DbName=db_name,
        DbUser=user,
        AutoCreate=True,
        **kwargs
    )
    user = response['DbUser']
    password = response['DbPassword']

    logging.info(f'Fetched credentials for user {user}')

    response = cli.describe_clusters(
        ClusterIdentifier=cluster_id
    )

    endpoint = response['Clusters'][0]['Endpoint']['Address']
    port = response['Clusters'][0]['Endpoint']['Port']

    return ConnectionDetails(
        endpoint=endpoint,
        db_name=db_name,
        port=port,
        user=user,
        password=password
    )


def get_connection_details_serverless(workgroup_name: str, db_name: str):
    cli = boto3.client("redshift-serverless")

    response: dict = cli.get_workgroup(workgroupName=workgroup_name)

    endpoint = response["workgroup"]["endpoint"]

    response = cli.get_credentials(
        workgroupName=workgroup_name,
        dbName=db_name,
    )

    return ConnectionDetails(
        endpoint=endpoint["address"],
        db_name=db_name,
        port=int(endpoint["port"]),
        user=response['dbUser'],
        password=response['dbPassword']
    )


def connect(connection_details):
    connection = psycopg2.connect(
        host=connection_details.endpoint,
        dbname=connection_details.db_name,
        user=connection_details.user,
        password=connection_details.password,
        port=connection_details.port,
        connection_factory=LoggingConnection
    )
    logger = logging.getLogger('connection_log')
    logger.setLevel(logging.DEBUG)
    connection.initialize(logger)
    return connection
