def split_table_name(table_name):
    if "." in table_name:
        schema, table_name = table_name.split(".", 1)
    else:
        schema = None
    return (schema, table_name)
