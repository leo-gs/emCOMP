'''
Examples:

f1 = Field("tweetId", "BIGINT(20)", is_primary_key=True)
f2 = Field("truncated", "BOOLEAN")
f3 = Field("isQuoteStatus", "BOOLEAN", is_primary_key=True)
f4 = Field("inReplyToStatusId", "BIGINT(20)")

t = Table("test", [f1, f2, f3, f4])
print(t.get_create_statement())

f5 = Field("tweetId", "BIGINT(20)", is_primary_key=True, foreign_key=f1, foreign_key_table=t)
t2 = Table("test2", [f5])
print(t2.get_create_statement())

'''


class Field():

	def __init__(self, name, datatype, is_primary_key=False, foreign_key=None, foreign_key_table=None):
		self.name = name
		self.datatype = datatype
		self.is_primary_key = is_primary_key
		self.foreign_key = foreign_key
		self.foreign_key_table = foreign_key_table
		
		assert bool(foreign_key) == bool(foreign_key_table), "If foreign_key is assigned, foreign_key_table must also be assigned"

	def get_insert_clause(self):
		return "{name} {datatype}".format(
				name = self.name,
				datatype = self.datatype
			)

	def get_foreign_key_clause(self):
		return "FOREIGN KEY ({field}) REFERENCES {other_table} ({other_field})".format(
				field=self.name,
				other_table=self.foreign_key_table.name,
				other_field=self.foreign_key.name
			)

class Table():

	def __init__(self, name, fields, prefix=""):
		if prefix:
			name = prefix + name
		self.name = name
		self.fields = fields

	def get_drop_statement(self, if_exists=False):
		if_exists_clause = " IF EXISTS"
		drop_statement = "DROP TABLE{if_exists} {table_name};".format(table_name=self.name, if_exists=if_exists_clause)
		return drop_statement

	def get_create_statement(self, if_not_exists=False):
		if_not_exists_clause = " IF NOT EXISTS" if if_not_exists else ""
		
		field_strs = [field.get_insert_clause() for field in self.fields]

		primary_keys = [field.name for field in self.fields if field.is_primary_key]
		if primary_keys:
			primary_key_clause = "PRIMARY KEY (" + ",".join(primary_key for primary_key in primary_keys) + ")"
			field_strs.append(primary_key_clause)

		foreign_keys = [field.get_foreign_key_clause() for field in self.fields if field.foreign_key]
		for foreign_key in foreign_keys:
			field_strs.append(foreign_key)

		create_statement = "CREATE TABLE{if_not_exists} {table_name}({fields});".format(
				if_not_exists=if_not_exists_clause,
				table_name=self.name,
				fields=", ".join(field_strs))

		return create_statement

	def get_insert_statement(self, on_duplicate_ignore=False):
		row_template = ",".join([field.name for field in self.fields])

		on_duplicate_key_ignore_clause = " ON DUPLICATE KEY IGNORE"

		insert_statement = "INSERT INTO {table_name} ({row_template}) VALUES ({value_flags}){on_duplicate_key_ignore}".format(
			table_name=self.name,
			row_template=row_template,
			value_flags=",".join(["%s" for field in self.fields]),
			on_duplicate_key_ignore=on_duplicate_key_ignore_clause if on_duplicate_ignore else "")

		return insert_statement

	def get_field(self, name):
		for field in self.fields:
			if field.name == name:
				return field


