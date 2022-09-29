#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
"""
import logging
from datetime import datetime
from typing import List, Optional, Dict, Tuple

import yaml
from sqlalchemy.engine.result import Row

from metric_model import MetricModel, DimensionModel, EnumMetricUnit
from query_config_model import QueryConfigModel


def load_queries(path: str) -> List[QueryConfigModel]:
    with open(path, 'r') as stream:
        try:
            parsed_yaml = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

    return [QueryConfigModel(data=entry) for entry in parsed_yaml["queries"]]


def gather_table_stats(conn, dimensions: List[DimensionModel] = None) -> List[MetricModel]:
    '''
    Gather metrics from SVV_TABLE_INFO
    See the link below to better understand the columns of this SVV
    https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_TABLE_INFO.html

    The goal is to build a metric of overall performance based on table stats
    '''

    # output
    metrics: List[MetricModel] = []
    # timestamp for metrics
    now = datetime.utcnow()

    # variables
    tables_not_compressed = 0
    max_skew_ratio = 0
    total_skew_ratio = 0
    number_tables_skew = 0
    number_tables = 0
    max_skew_sort_ratio = 0
    total_skew_sort_ratio = 0
    number_tables_skew_sort = 0
    number_tables_statsoff = 0
    max_varchar_size = 0
    max_unsorted_pct = 0
    total_rows = 0
    avg_skew_ratio = 0
    avg_skew_sort_ratio = 0

    schema_stats: Dict[str, Tuple[int, int]] = {}

    _sql = """
       SELECT DISTINCT
           \"schema\" || '.' || \"table\" as fqdn_table, 
           \"schema\" as schema_name, 
           encoded, 
           max_varchar, 
           unsorted, 
           stats_off, 
           tbl_rows, 
           skew_sortkey1, 
           skew_rows, 
           size 
       FROM svv_table_info
       """
    logging.info("Running query=TableStats statement={inline_sql}".format(inline_sql=_sql.replace('\n', ' ')))

    rows: List[Row] = conn.execute(_sql)
    for row in rows:
        (
            fqdn_table,
            schema_name,
            encoded,
            max_varchar,
            unsorted,
            stats_off,
            tbl_rows,
            skew_sortkey1,
            skew_rows,
            size
        ) = row
        number_tables += 1

        if encoded == 'N':
            tables_not_compressed += 1

        if skew_rows is not None:
            if skew_rows > max_skew_ratio:
                max_skew_ratio = skew_rows
            total_skew_ratio += skew_rows
            number_tables_skew += 1

        if skew_sortkey1 is not None:
            if skew_sortkey1 > max_skew_sort_ratio:
                max_skew_sort_ratio = skew_sortkey1
            total_skew_sort_ratio += skew_sortkey1
            number_tables_skew_sort += 1

        if stats_off is not None and stats_off > 5:
            number_tables_statsoff += 1

        # Size of the largest column that uses a VARCHAR data type.
        if max_varchar is not None and max_varchar > max_varchar_size:
            max_varchar_size = max_varchar

        if unsorted is not None and unsorted > max_unsorted_pct:
            max_unsorted_pct = unsorted

        if tbl_rows is not None:
            total_rows += tbl_rows

        # acc schema level metrics
        if schema_name not in schema_stats:
            schema_stats[schema_name] = tbl_rows, size
        else:
            acc_rows, acc_size = schema_stats[schema_name]
            schema_stats[schema_name] = acc_rows + tbl_rows, acc_size + size

        # fqdn-table analysis
        table_dimensions: List[DimensionModel] = [
            DimensionModel(name="SchemaName", value=schema_name),
            DimensionModel(name="TableName", value=fqdn_table),
        ]
        table_dimensions.extend(dimensions)
        metrics.extend([
            MetricModel(name="TableSize", value=size, unit=EnumMetricUnit.MEGABYTES, timestamp=now,
                        dimensions=table_dimensions),
            MetricModel(name="TableRows", value=tbl_rows, unit=EnumMetricUnit.COUNT, timestamp=now,
                        dimensions=table_dimensions),
        ])

    # schema analysis
    for schema_name, stats in schema_stats.items():
        acc_rows, acc_size = stats

        schema_dimensions: List[DimensionModel] = [DimensionModel(name="SchemaName", value=schema_name)]
        schema_dimensions.extend(dimensions)
        metrics.extend([
            MetricModel(name="SchemaSize", value=acc_size, unit=EnumMetricUnit.MEGABYTES, timestamp=now,
                        dimensions=schema_dimensions),
            MetricModel(name="SchemaRows", value=acc_rows, unit=EnumMetricUnit.COUNT, timestamp=now,
                        dimensions=schema_dimensions),
        ])

    # stats
    stats = [
        MetricModel(name='TablesNotCompressed', value=tables_not_compressed, unit=EnumMetricUnit.COUNT,
                    timestamp=now, dimensions=dimensions),
        MetricModel(name='MaxSkewRatio', value=max_skew_ratio, unit=EnumMetricUnit.PERCENT, timestamp=now,
                    dimensions=dimensions),
        MetricModel(name='MaxSkewSortRatio', value=max_skew_sort_ratio, unit=EnumMetricUnit.PERCENT, timestamp=now,
                    dimensions=dimensions),
        MetricModel(name='AvgSkewRatio', value=avg_skew_ratio, unit=EnumMetricUnit.PERCENT, timestamp=now,
                    dimensions=dimensions),
        MetricModel(name='AvgSkewSortRatio', value=avg_skew_sort_ratio, unit=EnumMetricUnit.PERCENT, timestamp=now,
                    dimensions=dimensions),
        MetricModel(name='Tables', value=number_tables, unit=EnumMetricUnit.COUNT, timestamp=now,
                    dimensions=dimensions),
        MetricModel(name='Rows', value=total_rows, unit=EnumMetricUnit.COUNT, timestamp=now,
                    dimensions=dimensions),
        MetricModel(name='TablesStatsOff', value=number_tables_statsoff, unit=EnumMetricUnit.COUNT,
                    timestamp=now, dimensions=dimensions),
        MetricModel(name='MaxVarcharSize', value=max_varchar_size, unit=EnumMetricUnit.NONE, timestamp=now,
                    dimensions=dimensions),
        MetricModel(name='MaxUnsorted', value=max_unsorted_pct, unit=EnumMetricUnit.PERCENT, timestamp=now,
                    dimensions=dimensions),
    ]
    metrics.extend(stats)

    return metrics


def gather_service_class_stats(conn, dimensions: List[DimensionModel] = None) -> List[MetricModel]:
    _sql = """
        SELECT DATE_TRUNC('hour', a.service_class_start_time)          AS metrics_ts,
               TRIM(d.name)                                            as service_class,
               COUNT(a.query)                                          AS query_count,
               SUM(a.total_exec_time)                                  AS sum_exec_time,
               sum(case when a.total_queue_time > 0 then 1 else 0 end) AS count_queued_queries,
               SUM(a.total_queue_time)                                 AS sum_queue_time,
               count(c.is_diskbased)                                   as count_diskbased_segments
        FROM stl_wlm_query a
                 JOIN stv_wlm_classification_config b ON a.service_class = b.action_service_class
                 LEFT OUTER JOIN (select query, SUM(CASE when is_diskbased = 't' then 1 else 0 end) is_diskbased
                                  from svl_query_summary
                                  group by query) c on a.query = c.query
                 JOIN stv_wlm_service_class_config d on a.service_class = d.service_class
        WHERE a.service_class > 5
          AND a.service_class_start_time > DATEADD(hour, -2, current_date)
        GROUP BY DATE_TRUNC('hour', a.service_class_start_time),
                 d.name  
    """

    logging.info("Running query=WLM statement={inline_sql}".format(inline_sql=_sql.replace('\n', ' ')))

    # return
    metrics: List[MetricModel] = []
    rows: List[Row] = conn.execute(_sql)
    for row in rows:
        metrics_ts, service_class, query_count, sum_exec_time, count_queued_queries, sum_queue_time, count_diskbased_segments = row

        dims: List[DimensionModel] = [
            DimensionModel(name="ServiceClassID", value=service_class)
        ]
        dims.extend(dimensions)

        metrics.extend([
            MetricModel(name='ServiceClass-Queued', value=count_queued_queries, timestamp=metrics_ts,
                        unit=EnumMetricUnit.COUNT,
                        dimensions=dims),
            MetricModel(name='ServiceClass-QueueTime', value=sum_queue_time, timestamp=metrics_ts,
                        unit=EnumMetricUnit.COUNT,
                        dimensions=dims),
            MetricModel(name='ServiceClass-Executed', value=query_count, timestamp=metrics_ts,
                        unit=EnumMetricUnit.COUNT,
                        dimensions=dims),
            MetricModel(name='ServiceClass-ExecTime', value=sum_exec_time, timestamp=metrics_ts,
                        unit=EnumMetricUnit.COUNT,
                        dimensions=dims),
            MetricModel(name='ServiceClass-DiskbasedQuerySegments', value=count_diskbased_segments,
                        unit=EnumMetricUnit.COUNT,
                        timestamp=metrics_ts,
                        dimensions=dims),
        ])

    return metrics


def gather_user_group_stats(conn, dimensions: List[DimensionModel] = None) -> List[MetricModel]:
    # output
    metrics: List[MetricModel] = []
    # timestamp for metrics
    now = datetime.utcnow()

    # because user-group info is a table stored in the leader node,
    # we have to hack query by splitting it into two
    # [0A000] ERROR: Specified types or functions (one per INFO message) not supported on Redshift tables.
    _sql = """
        select distinct
            usesysid as user_id, 
            usename as user_name
        from pg_user,
             pg_group
        where pg_user.usesysid = ANY(pg_group.grolist)
    """

    logging.info(
        "Running query=UserGroupMapping statement={inline_sql}".format(inline_sql=_sql.replace('\n', ' ').strip()))

    user_info: Dict[str, str] = {}
    rows: List[Row] = conn.execute(_sql)
    for row in rows:
        user_id, user_name = row
        user_info[user_id] = user_name

    _sql = """
        SELECT DISTINCT a.userid       as user_id,
                        COUNT(a.query) as query_count
        FROM SVL_QUERY_SUMMARY a
        GROUP BY user_id
    """

    logging.info(
        "Running query=UserQuerySummaryCount statement={inline_sql}".format(inline_sql=_sql.replace('\n', ' ').strip()))

    rows: List[Row] = conn.execute(_sql)
    for row in rows:
        user_id, query_count = row

        if user_id in user_info:
            user_name = user_info[user_id]

            user_dimensions: List[DimensionModel] = [
                DimensionModel(name="UserName", value=user_name),
            ]
            user_dimensions.extend(dimensions)
            metrics.append(
                MetricModel(name="UserQuerySummaryCount", value=query_count, unit=EnumMetricUnit.COUNT, timestamp=now,
                            dimensions=user_dimensions)
            )

    # running queries
    _sql: str = """
        SELECT 
            user_name, 
            status, 
            count(query) as query_count
        FROM stv_recents
        GROUP BY user_name, status;
    """
    logging.info(
        "Running query=UserQueryRecentCount statement={inline_sql}".format(inline_sql=_sql.replace('\n', ' ').strip()))
    rows: List[Row] = conn.execute(_sql)
    for row in rows:
        user_name, status, query_count = row

        user_dimensions: List[DimensionModel] = [
            DimensionModel(name="UserName", value=user_name),
        ]
        user_dimensions.extend(dimensions)

        if status == "Running":
            metric_name = "UserQueryRecentRunningCount"
        else:
            metric_name = "UserQueryRecentDoneCount"

        m = MetricModel(name=metric_name, value=query_count, unit=EnumMetricUnit.COUNT, timestamp=now,
                        dimensions=user_dimensions)
        metrics.append(m)

    return metrics


def run_custom_query(conn, query: QueryConfigModel, dimensions: List[DimensionModel]) -> Optional[MetricModel]:
    _ts = datetime.now()
    _value = conn.execute(query.statement).fetchone()[0]
    _interval = (datetime.now() - _ts).microseconds / 1000

    result: MetricModel = None
    if _value is not None:
        if query.type == "Query":
            result = MetricModel(name=query.name, value=_value, timestamp=_ts, unit=query.get_unit(),
                                 dimensions=dimensions)
        else:
            result = MetricModel(name=query.name, value=_interval, timestamp=_ts, unit=EnumMetricUnit.MILLISECONDS,
                                 dimensions=dimensions)

    else:
        logging.warning(f"Query {query.get_name()} had Nothing to report value=None")

    if result:
        return result
    return None
