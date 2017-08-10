using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace Raven.Server.SqlMigration
{
    public class SqlDatabase
    {
        public string Name;
        public readonly SqlConnection Connection;
        public List<SqlTable> Tables;

        public SqlDatabase(SqlConnection con)
        {
            Connection = con;
            Name = Connection.Database;
            Tables = new List<SqlTable>();

            SetTables();
            SetKeysRelations();
            SetPrimaryKeys();
            SetUnsuppoertedColumns();
        }

        private void SetUnsuppoertedColumns()
        {
            var query = @"select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where DATA_TYPE = 'hierarchyid' or DATA_TYPE = 'geography'";

            using (var cmd = new SqlCommand(query, Connection))
            {
                using (var reader = SqlHelper.ExecuteReader(cmd))
                {
                    while (reader.Read())
                        GetTableByName(GetTableNameWithSchema(reader["TABLE_SCHEMA"].ToString(), reader["TABLE_NAME"].ToString())).UnsupportedColumns.Add(reader["COLUMN_NAME"].ToString());
                }
            }
        }

        public void Embed(string parentTableName, string propertyName, string childTableName)
        {
            SqlTable parentTable;
            SqlTable childTable;

            if (!TryGetTableByName(parentTableName, out parentTable))
                throw new InvalidOperationException($"The table '{parentTableName}' does not exists.");

            if (!TryGetTableByName(childTableName, out childTable))
                throw new InvalidOperationException($"The table '{childTableName}' does not exists.");

            foreach (var reference in childTable.References)
                if (reference.Value.Item1 == parentTableName)
                {
                    childTable.IsEmbedded = true;
                    parentTable.AddEmbeddedTable(propertyName, childTableName);
                    return;
                }

            throw new InvalidOperationException($"The table '{parentTableName}' cannot embed the table '{childTableName}'");
        }

        public void SetKeysRelations()
        {
            var referentialConstraints = new Dictionary<string, string>();

            var _query = "select CONSTRAINT_NAME, UNIQUE_CONSTRAINT_NAME from information_schema.REFERENTIAL_CONSTRAINTS";

            using (var cmd = new SqlCommand(_query, Connection))
            {
                using (var reader = SqlHelper.ExecuteReader(cmd))
                {
                    while (reader.Read())
                        referentialConstraints.Add(reader["CONSTRAINT_NAME"].ToString(), reader["UNIQUE_CONSTRAINT_NAME"].ToString());
                }
            }

            foreach (var kvp in referentialConstraints)
            {
                string parentTableName;
                string parentColumnName;
                string childTableName;
                string childColumnName;

                var query = "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME from information_schema.KEY_COLUMN_USAGE where CONSTRAINT_NAME = @constraintName";
                using (var cmd = new SqlCommand(query, Connection))
                {
                    cmd.Parameters.AddWithValue("constraintName", kvp.Key);
                    using (var reader = SqlHelper.ExecuteReader(cmd))
                    {
                        if (reader.Read())
                        {
                            parentTableName = GetTableNameWithSchema(reader["TABLE_SCHEMA"].ToString(), reader["TABLE_NAME"].ToString());
                            parentColumnName = reader["COLUMN_NAME"].ToString();
                        }
                        else
                            continue;
                    }
                }

                query = "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME from information_schema.KEY_COLUMN_USAGE where CONSTRAINT_NAME = @constraintName";
                using (var cmd = new SqlCommand(query, Connection))
                {
                    cmd.Parameters.AddWithValue("constraintName", kvp.Value);
                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            childTableName = GetTableNameWithSchema(reader["TABLE_SCHEMA"].ToString(), reader["TABLE_NAME"].ToString());
                            childColumnName = reader["COLUMN_NAME"].ToString();
                        }
                        else
                            continue;
                    }
                }

                
                var table = GetTableByName(parentTableName);
                var childTable = GetTableByName(childTableName);
                childTable.IsReferenced = true;
                table.References.Add(parentColumnName, new Tuple<string, string>(childTableName, childColumnName));
            }
        }

        private void SetPrimaryKeys()
        {
            var query = @"SELECT tc.TABLE_SCHEMA, tc.TABLE_NAME, COLUMN_NAME
                            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
                            INNER JOIN
                            INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
                            ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
                            TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME";

            using (var cmd = new SqlCommand(query, Connection))
            {
                using (var reader = SqlHelper.ExecuteReader(cmd))
                {
                    while (reader.Read())
                        GetTableByName(GetTableNameWithSchema(reader["TABLE_SCHEMA"].ToString(), reader["TABLE_NAME"].ToString())).PrimaryKeys.Add(reader["COLUMN_NAME"].ToString());
                }
            }
        }     

        private void SetTables()
        {
            var query = "select TABLE_SCHEMA, TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_TYPE = @tableType";

            var lst = new List<string>();

            using (var cmd = new SqlCommand(query, Connection))
            {
                cmd.Parameters.AddWithValue("tableType", "BASE TABLE");
                using (var reader = SqlHelper.ExecuteReader(cmd))
                {
                    while (reader.Read())
                        lst.Add(GetTableNameWithSchema(reader["TABLE_SCHEMA"].ToString(), reader["TABLE_NAME"].ToString()));
                }
            }
            foreach (var item in lst)
                Tables.Add(new SqlTable(item));
        }

        private string GetTableNameWithSchema(string schemaName, string tableName)
        {
            return $"{schemaName}.{tableName}";
        }

        public bool TryGetTableByName(string name, out SqlTable table)
        {
            foreach (var tbl in Tables)
                if (tbl.Name == name)
                {
                    table = tbl;
                    return true;
                }

            table = null;
            return false;
        }

        public SqlTable GetTableByName(string tableName)
        {
            foreach (var table in Tables)
                if (table.Name == tableName)
                    return table;

            return null;
        }


        public string TableQuote(string q)
        {
            q = $"[{q}]";
            
            q = q.Insert(q.IndexOf('.'), "]");
            q = q.Insert(q.IndexOf('.') + 1, "[");

            return q;
        }
    }
}
