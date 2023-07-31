import logging
import sqlalchemy.sql as sas

from plpipes.database.backend import Backend
from plpipes.database.sqlext import AsSubquery, Wrap

class DictBackend(Backend):
    def query(self, txn, sql, parameters, kws):
        result = txn._conn.execute(Wrap(sql), parameters=parameters, **kws)
        return [row._mapping for row in result.all()]

    def query_first(self, txn, sql, parameters, kws):
        for row in txn._conn.execute(Wrap(sql), parameters=parameters, **kws):
            return row._mapping

    def register_handlers(self, handlers):
        # TODO
        # handlers["create_table"].register(list), self._create_table_from_list)
        # handlers["create_table"].register(dict), self._create_table_from_dict)
        pass
