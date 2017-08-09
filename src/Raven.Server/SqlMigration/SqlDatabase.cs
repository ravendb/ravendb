using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace Raven.Server.SqlMigration
{
    class SqlDatabase
    {
        public string Name;
        public readonly SqlConnection Connection;
        public List<SqlTable> Tables;

        public SqlDatabase(SqlConnection con)
        {
            Connection = con;
            Name = Connection.Database;
            Tables = new List<SqlTable>();

            SetTablesList();
            SetKeysRelations();
            SetPrimaryKeys();
        }

        internal void Embed(string parentTableName, string propertyName, string childTableName)
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
            var ReferentialConstraints = new Dictionary<string, string>();

            var _query = "select CONSTRAINT_NAME, UNIQUE_CONSTRAINT_NAME from information_schema.REFERENTIAL_CONSTRAINTS";

            using (var cmd = new SqlCommand(_query, Connection))
            {
                using (var reader = SqlHelper.ExecuteReader(cmd))
                {
                    while (reader.Read())
                        ReferentialConstraints.Add(reader["CONSTRAINT_NAME"].ToString(), reader["UNIQUE_CONSTRAINT_NAME"].ToString());
                }
            }

            foreach (var kvp in ReferentialConstraints)
            {
                var parentTableName = string.Empty;
                var parentColumnName = string.Empty;
                var childTableName = string.Empty;
                var childColumnName = string.Empty;

                var query = "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME from information_schema.KEY_COLUMN_USAGE where CONSTRAINT_NAME = @constraintName";
                using (var cmd = new SqlCommand(query, Connection))
                {
                    cmd.Parameters.AddWithValue("constraintName", kvp.Key);
                    using (var reader = SqlHelper.ExecuteReader(cmd))
                    {
                        if (reader.Read())
                        {
                            parentTableName = $"{reader["TABLE_SCHEMA"]}.{reader["TABLE_NAME"]}";
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
                            childTableName = $"{reader["TABLE_SCHEMA"]}.{reader["TABLE_NAME"]}";
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
                        GetTableByName($"{reader["TABLE_SCHEMA"]}.{reader["TABLE_NAME"]}").PrimaryKeys.Add(reader["COLUMN_NAME"].ToString());
                }
            }
        }     

        private void SetTablesList()
        {
            var query = "select TABLE_SCHEMA, TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_TYPE = @tableType";
            var lst = new List<string>();

            using (var cmd = new SqlCommand(query, Connection))
            {
                cmd.Parameters.AddWithValue("tableType", "BASE TABLE");
                using (var reader = SqlHelper.ExecuteReader(cmd))
                {
                    while (reader.Read())
                        lst.Add($"{reader["TABLE_SCHEMA"]}.{reader["TABLE_NAME"]}");
                }
            }

            foreach (var name in lst)
                Tables.Add(new SqlTable(this, name));
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
    }
}
