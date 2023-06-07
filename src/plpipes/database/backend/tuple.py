import logging
import sqlalchemy.sql as sas

from plpipes.database.backend import Backend
from plpipes.database.sqlext import AsSubquery, Wrap

class TupleBackend(Backend):
    def query(self, txn, sql, parameters, kws):
        return txn._conn.execute(Wrap(sql), parameters=parameters, **kws).all()

    def query_first(self, txn, sql, parameters, kws):
        for row in txn._conn.execute(Wrap(sql), parameters=parameters, **kws):
            return row

    def query_first_value(self, txn, sql, parameters, kws):
        return self.query_first(txn, sql, parameters, kws)[0]

    def register_handlers(self, handlers):
        # TODO
        # handlers["create_table"].register(list), self._create_table_from_list)
        # handlers["create_table"].register(dict), self._create_table_from_dict)
        pass
