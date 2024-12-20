#!/usr/bin/env python3
"""
This module have a utility function that lists all documents in a collection.
"""
import pymongo


def list_all(mongo_collection):
    """
    list all documents in a collection
    """
    if mongo_collection is not None:
        return list(mongo_collection.find())
    return []

