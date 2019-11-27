from typing import List, Optional
from pymongo import DeleteMany, UpdateOne
from pymongo.collection import Collection


def update(
    coll: Collection, data: List[dict], update_keys: List[str], upsert=True
) -> None:
    """ Update data in the collection.

    Args:
        coll: The collection to save data.
        data: a list of data we will update.
        update_keys: indicate the key to identify elements.
        upsert: If data not existed, new record will be inserted.

    Examples:
        # update a list of user information into collection User by `name`.
        update(user_coll, [{"name": "gladiator", "age": 19}, {"name": "zero", "age": 30}], "name")

    Raises:
        KeyError will be raised if one of `update_keys` not in `data`
    """
    # remove duplicate dictionary in data
    data = [dict(t) for t in {tuple(d.items()) for d in data}]
    updates: List[UpdateOne] = []
    for item in data:
        update_conditions = {key: item[key] for key in update_keys}
        updates.append(UpdateOne(update_conditions, {"$set": item}, upsert=upsert))

    if not updates:
        return None
    coll.bulk_write(updates)
