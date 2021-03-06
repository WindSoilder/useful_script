from typing import List, Optional
from pymongo import DeleteMany
from pymongo.collection import Collection


def mongo_drop_duplicate(
    collection: Collection,
    keys: List[str],
    restricted: Optional[str] = None,
    bulk_size: Optional[int] = None,
):
    """ drop mongodb duplicate documents inside keys
    WARNING: You need to assure that the keys is index field in the collection, or this function's
    performance is un-acceptable.
    PLEASE NOTE THAT IT WILL DELETE DATA IN THE COLLECTION!

    Args:
        collection: The collection you want to drop duplicate content
        keys: The keys which you consider should be unique in the collection
        restricted: mongo query string, which can let you restrict the documents set
        bulk_size: when the parameter is None, the function will send `bulk_size` requests to mongo once

    Example:
        # suppose that you have a collection named exchange_information, which contain each stock's exchange information
        # they should have unique key (stock_id, date), then you can invoke the method like this:
        drop_mongo_duplicate(exchange_information, ('stock_id', 'date'))

        # for better performance
        drop_mongo_duplicate(exchange_information, ('stock_id', 'date'), bulk_size=1024)

        # if you want to drop duplicate date which is later than 2018-01-01
        drop_mongo_duplicate(exchange_information, ('stock_id', 'date'), {'date': {'$gt': datetime.datetime(2018, 1, 1)}}, bulk_size=1024)
    """

    def _extract_condition(record):
        condition = {key: value for key, value in record["_id"].items()}
        unique_id = collection.find_one(condition, {"_id": 1})["_id"]
        condition["_id"] = {"$ne": unique_id}
        return condition

    def _drop_in_bulk():
        op_buffer = []
        for res in results:
            condition = _extract_condition(res)
            if len(op_buffer) == bulk_size:
                collection.bulk_write(op_buffer)
                op_buffer = []
            else:
                op_buffer.append(DeleteMany(condition))
        # do operations on remain data
        if op_buffer:
            collection.bulk_write(op_buffer)

    def _drop_one_by_one():
        for res in results:
            condition = _extract_condition(res)
            collection.delete_many(condition)

    # Duplicates considered as count greater than one
    DUPLICATE_NUMBER = 1
    # the collection may be too huge to aggregate, for this situation, we need to parse
    # `allowDiskUse=True` to mongo server.
    if restricted:
        results = collection.aggregate(
            [
                {"$match": restricted},
                {"$group": {"_id": {k: f"${k}" for k in keys}, "total": {"$sum": 1}}},
                {"$match": {"total": {"$gt": DUPLICATE_NUMBER}}},
            ],
            allowDiskUse=True,
        )
    else:
        results = collection.aggregate(
            [
                {"$group": {"_id": {k: f"${k}" for k in keys}, "total": {"$sum": 1}}},
                {"$match": {"total": {"$gt": DUPLICATE_NUMBER}}},
            ],
            allowDiskUse=True,
        )

    if bulk_size:
        _drop_in_bulk()
    else:
        _drop_one_by_one()
