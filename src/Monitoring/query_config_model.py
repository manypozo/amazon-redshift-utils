#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Description:
"""


class QueryConfigModel:
    def __init__(self, data: dict):
        self.name: str = str(data["name"]).strip()
        self.unit: str = str(data["unit"]).strip()
        self.type: str = str(data["type"]).strip()
        self.statement: str = str(data["query"]).strip()

        if "description" in data and data["description"]:
            self.description: str = data["description"]
        else:
            self.description = None

    def get_name(self):
        return self.name

    def get_unit(self):
        return self.unit.capitalize()

    def get_type(self):
        return self.type.capitalize()

    def get_statement(self, inline: bool = False):
        if inline:
            return self.statement.replace("\n", " ")
        return self.statement
