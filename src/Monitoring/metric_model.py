#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
"""
from datetime import datetime
from enum import Enum
from typing import List


class EnumMetricUnit(str, Enum):
    SECONDS = 'Seconds'
    MICROSECONDS = 'Microseconds'
    MILLISECONDS = 'Milliseconds'
    BYTES = 'Bytes'
    KILOBYTES = 'Kilobytes'
    MEGABYTES = 'Megabytes'
    GIGABYTES = 'Gigabytes'
    TERABYTES = 'Terabytes'
    BITS = 'Bits'
    KILOBITS = 'Kilobits'
    MEGABITS = 'Megabits'
    GIGABITS = 'Gigabits'
    TERABITS = 'Terabits'
    PERCENT = 'Percent'
    COUNT = 'Count'
    BYTES_SECOND = 'Bytes/Second'
    KILOBYTES_SECOND = 'Kilobytes/Second'
    MEGABYTES_SECOND = 'Megabytes/Second'
    GIGABYTES_SECOND = 'Gigabytes/Second'
    TERABYTES_SECOND = 'Terabytes/Second'
    BITS_SECOND = 'Bits/Second'
    KILOBITS_SECOND = 'Kilobits/Second'
    MEGABITS_SECOND = 'Megabits/Second'
    GIGABITS_SECOND = 'Gigabits/Second'
    TERABITS_SECOND = 'Terabits/Second'
    COUNT_SECOND = 'Count/Second'
    NONE = 'None'


class DimensionModel:
    def __init__(self, name: str, value: any):
        self.name = name
        self.value = value

    def aws_dimension_format(self):
        data: dict = {
            'Name': self.name,
            'Value': self.value,
        }
        return data


class MetricModel:
    def __init__(self, name: str, value: int or float, timestamp: datetime,
                 unit: EnumMetricUnit or str = EnumMetricUnit.NONE,
                 dimensions: List[DimensionModel] = None,
                 description: str = None):
        self.name = name
        self.value = value
        self.timestamp = timestamp

        if not description:
            self.description = name

        if isinstance(unit, EnumMetricUnit):
            self.unit: str = unit.value
        else:
            self.unit: str = unit

        if not dimensions:
            dimensions = []
        self.dimensions = dimensions

    def to_dict(self) -> dict:
        d: dict = self.__dict__.copy()
        d.__delitem__("name")
        d["statement_name"] = self.name
        d["dimensions"] = [dim.__dict__ for dim in self.dimensions]
        return d

    def aws_metric_format(self):
        data: dict = {
            'MetricName': self.name,
            'Dimensions': [d.aws_dimension_format() for d in self.dimensions],
            'Timestamp': self.timestamp,
            'Value': self.value,
        }
        if self.unit is not None and self.unit is not EnumMetricUnit.NONE.value:
            data["Unit"] = self.unit

        return data

#     def __build_metric(name: str, value, unit, ts, cluster_id: str):
#         return {
#             'MetricName': name,
#             'Dimensions': [
#                 {'Name': 'ClusterIdentifier', 'Value': cluster_id}
#             ],
#             'Timestamp': ts,
#             'Value': value,
#             'Unit': unit
#         }
