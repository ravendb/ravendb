using System;
using System.Collections.Generic;
using System.Data;
using Raven.Server.Documents;
using Sparrow.Json;

namespace Raven.Server.SqlMigration
{
    public partial class SqlDatabase
    {
        public readonly DocumentDatabase DocumentDatabase;
        public readonly List<SqlTable> Tables;
        public readonly IDbConnection Connection;
        public readonly RavenDocumentFactory Factory;

        private readonly Validator _validator;

        public SqlDatabase(string connectionString, DocumentDatabase documentDatabase, RavenDocumentFactory factory, BlittableJsonReaderArray tablesToWrite)
        {
            Connection = ConnectionFactory.OpenConnection(connectionString);
            DocumentDatabase = documentDatabase;
            SetTablesFromBlittableArray(tablesToWrite, ref Tables);

            Factory = factory;
            _validator = new Validator(this, Factory);

            SetPrimaryKeys();
            SetForeignKeys();
        }

        private void SetTablesFromBlittableArray(BlittableJsonReaderArray tablesToWrite, ref List<SqlTable> tables, SqlTable parentTable = null)
        {
            if (tablesToWrite == null)
                return;

            if (tables == null)
                tables = new List<SqlTable>();

            foreach (BlittableJsonReaderObject item in tablesToWrite.Items)
            {
                item.TryGet("Name", out string name);
                item.TryGet("Query", out string childQuery);
                item.TryGet("Patch", out string patchScript);
                item.TryGet("Property", out string propertyName);

                var table = new SqlTable(name, childQuery, patchScript, Connection);
                parentTable?.Embed(table, propertyName);

                if (item.TryGet("Embedded", out BlittableJsonReaderArray childEmbeddedTables))
                    SetTablesFromBlittableArray(childEmbeddedTables, ref tables, table);

                tables.Add(table);
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

            using (var reader = new SqlReader(connection, Queries.SelectAllTables))
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

            using (var reader = new SqlReader(Connection, Queries.SelectReferantialConstraints))
            {
                while (reader.Read())
                    referentialConstraints.Add(reader["CONSTRAINT_NAME"].ToString(), reader["UNIQUE_CONSTRAINT_NAME"].ToString());
            }

            foreach (var kvp in referentialConstraints)
            {
                string parentTableName;
                var parentColumnName = new List<string>();
                var childTableName = new List<string>();

                using (var reader = new SqlReader(Connection, Queries.SelectKeyColumnUsageWhereConstraintName))
                {
                    reader.AddParameter("constraintName", kvp.Key);

                    if (!reader.Read())
                        continue;

                    do
                    {
                        parentTableName = GetTableNameFromReader(reader);
                        parentColumnName.Add(reader["COLUMN_NAME"].ToString());
                    }
                    while (reader.Read());
                }

                using (var reader = new SqlReader(Connection, Queries.SelectKeyColumnUsageWhereConstraintName))
                {
                    reader.AddParameter("constraintName", kvp.Value);

                    if (!reader.Read())
                        continue;

                    do
                        childTableName.Add(GetTableNameFromReader(reader));
                    while (reader.Read());
                }

                var temp = GetAllTablesByName(parentTableName);

                foreach (var table in temp)
                    for (var i = 0; i < parentColumnName.Count; i++)
                        if (!table.ForeignKeys.TryAdd(parentColumnName[i], childTableName[i]))
                            throw new InvalidOperationException($"Column '{parentColumnName}' cannot reference multiple tables.");
            }
        }

        private void SetPrimaryKeys()
        {
            using (var reader = new SqlReader(Connection, Queries.SelectPrimaryKeys))
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
