from plpipes.plugin import Plugin, plugin

from databricks.connect import DatabricksSession

@plugin
class DatabricksPlugin(Plugin):

    def init_spark_session(self, ssc):
        profile = ssc.get("profile", "DEFAULT")
        builder = DatabricksSession.builder.profile(profile)

        for k, v in ssc.to_flat_dict('extra').items():
            builder = builder.config(k, v)

        ss = builder.getOrCreate()

        default_database = ssc.get("default_database", None)
        if default_database is not None:
            ss.sql(f"USE {default_database}")

        return ss
