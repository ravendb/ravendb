using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.SqlMigration
{
    public partial class SqlDatabase
    {
        public List<SqlTable> Tables;
        public readonly IDbConnection Connection;
        public readonly SqlMigrationDocumentFactory Factory;

        private readonly Validator _validator;

        public SqlDatabase(SqlConnection connection, SqlMigrationDocumentFactory factory, List<SqlMigrationImportOperation.SqlMigrationTable> tablesToWrite)
        {
            Connection = connection;
            Tables = new List<SqlTable>();
            SetTablesFromBlittableArray(tablesToWrite);

            Factory = factory;
            _validator = new Validator(this);

            SetPrimaryKeys();
            SetForeignKeys();
        }

        private void SetTablesFromBlittableArray(List<SqlMigrationImportOperation.SqlMigrationTable> tablesToWrite, SqlTable parentTable = null)
        {
            foreach (var item in tablesToWrite)
            {
                SqlTable table;

                if (parentTable != null)
                {
                    table = new SqlEmbeddedTable(item.Name, item.Query, item.Patch, parentTable.Name, item.Property, Connection);
                    parentTable.EmbeddedTables.Add((SqlEmbeddedTable) table);
                }
                else table = new SqlTable(item.Name, item.Query, item.Patch, Connection);

                if (item.EmbeddedTables != null)
                    SetTablesFromBlittableArray(item.EmbeddedTables, table);

                Tables.Add(table);
            }
        }

        public bool IsValid()
        {
            return _validator.IsValid;
        }

        public void Validate(out List<string> errors)
        {
            _validator.Validate(out var validationErrors);

            errors = validationErrors;
        }

        private static string GetTableNameFromReader(SqlReader reader)
        {
            var tableName = reader["TABLE_NAME"].ToString();
            var schemaName = reader["TABLE_SCHEMA"].ToString();

            return $"{schemaName}.{tableName}";
        }

        public static List<string> GetAllTablesNamesFromDatabase(IDbConnection connection)
        {
            var lst = new List<string>();

            using (var reader = new SqlReader(connection, SqlQueries.SelectAllTables))
            {
                reader.AddParameter("tableType", "BASE TABLE");

                while (reader.Read())
                    lst.Add(GetTableNameFromReader(reader));
            }

            return lst;
        }

        private void SetForeignKeys()
        {
            var referentialConstraints = new Dictionary<string, string>();

            using (var reader = new SqlReader(Connection, SqlQueries.SelectReferantialConstraints))
            {
                while (reader.Read())
                    referentialConstraints.Add(reader["CONSTRAINT_NAME"].ToString(), reader["UNIQUE_CONSTRAINT_NAME"].ToString());
            }

            foreach (var kvp in referentialConstraints)
            {
                string parentTableName;
                var parentColumnName = new List<string>();
                var childTableName = new List<string>();

                using (var reader = new SqlReader(Connection, SqlQueries.SelectKeyColumnUsageWhereConstraintName))
                {
                    reader.AddParameter("constraintName", kvp.Key);

                    if (reader.Read() == false)
                        continue;

                    do
                    {
                        parentTableName = GetTableNameFromReader(reader);
                        parentColumnName.Add(reader["COLUMN_NAME"].ToString());
                    }
                    while (reader.Read());
                }

                using (var reader = new SqlReader(Connection, SqlQueries.SelectKeyColumnUsageWhereConstraintName))
                {
                    reader.AddParameter("constraintName", kvp.Value);

                    if (reader.Read() == false)
                        continue;

                    do
                        childTableName.Add(GetTableNameFromReader(reader));
                    while (reader.Read());
                }

                var temp = GetAllTablesByName(parentTableName);

                if (temp.Any(table => parentColumnName.Where((t, i) => table.ForeignKeys.TryAdd(t, childTableName[i]) == false).Any()))
                    throw new InvalidOperationException($"Column '{parentColumnName}' cannot reference multiple tables.");
            }
        }

        private void SetPrimaryKeys()
        {
            using (var reader = new SqlReader(Connection, SqlQueries.SelectPrimaryKeys))
            {
                while (reader.Read())
                {
                    var lst = GetAllTablesByName(GetTableNameFromReader(reader));

                    foreach (var table in lst)
                        table.PrimaryKeys.Add(reader["COLUMN_NAME"].ToString());
                }
            }
        }

        private List<SqlTable> GetAllTablesByName(string tableName)
        {
            var lst = new List<SqlTable>();

            foreach (var table in Tables)
                if (table.Name == tableName)
                    lst.Add(table);

            return lst;
        }
    }
}
