using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Raven.Client.Documents.Operations.Migration;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.SqlMigration
{
    public partial class SqlDatabase
    {
        public List<SqlParentTable> ParentTables;
        public List<SqlEmbeddedTable> EmbeddedTables;
        public readonly IDbConnection Connection;
        public readonly SqlMigrationDocumentFactory Factory;
        public readonly DocumentsOperationContext Context;
        public readonly string ConnectionString;
        private readonly Validator _validator;

        public SqlDatabase(IDbConnection connection, string connectionString)
        {
            ConnectionString = connectionString;
            Connection = connection;
            ParentTables = new List<SqlParentTable>();
            EmbeddedTables = new List<SqlEmbeddedTable>();

            var names = GetAllTablesNamesFromDatabase(Connection);
            foreach (var name in names)
                ParentTables.Add(new SqlParentTable(name, null, this, null, null));

            SetPrimaryKeys();
            SetForeignKeys();
        }

        public SqlDatabase(IDbConnection connection, string connectionString, SqlMigrationDocumentFactory factory, DocumentsOperationContext context, List<SqlMigrationImportOperation.SqlMigrationTable> tablesToWrite)
        {
            ConnectionString = connectionString;
            Connection = connection;
            ParentTables = new List<SqlParentTable>();
            EmbeddedTables = new List<SqlEmbeddedTable>();
            Context = context;

            SetTablesFromBlittableArray(tablesToWrite);

            Factory = factory;
            _validator = new Validator(this, context);

            SetPrimaryKeys();
            SetForeignKeys();
        }

        public List<SqlTable> GetAllTables()
        {
            var lst = new List<SqlTable>(ParentTables);
            lst.AddRange(new List<SqlTable>(EmbeddedTables));
            return lst;
        }

        private void SetTablesFromBlittableArray(List<SqlMigrationImportOperation.SqlMigrationTable> tablesToWrite, SqlTable parentTable = null)
        {
            foreach (var item in tablesToWrite)
            {
                SqlTable table;

                if (parentTable != null)
                {
                    table = new SqlEmbeddedTable(item.Name, item.Query, this, item.NewName, parentTable.Name);
                    parentTable.EmbeddedTables.Add((SqlEmbeddedTable)table);
                    EmbeddedTables.Add((SqlEmbeddedTable)table);
                }
                else
                {
                    table = new SqlParentTable(item.Name, item.Query, this, item.NewName, item.Patch);
                    ParentTables.Add((SqlParentTable)table);
                }

                if (item.EmbeddedTables != null)
                    SetTablesFromBlittableArray(item.EmbeddedTables, table);
            }
        }

        public bool IsValid()
        {
            return _validator.IsValid;
        }

        public void Validate(out List<SqlMigrationImportResult.Error> errors)
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

        public static Dictionary<string, List<string>> GetSchemaResultTablesColumns(IDbConnection connection)
        {
            using (var reader = new SqlReader(connection, SqlQueries.SelectColumns))
            {
                var temp = new Dictionary<string, List<string>>();

                while (reader.Read())
                {
                    var name = GetTableNameFromReader(reader);
                    if (temp.ContainsKey(name) == false)
                        temp[name] = new List<string>();
                    temp[name].Add(reader["COLUMN_NAME"].ToString());
                }

                return temp;
            }
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
            return GetAllTables().Where(table => table.Name == tableName).ToList();
        }

        public string GetParentTableNewName(string tableName)
        {
            return (from table in ParentTables
                    where table.Name == tableName
                    select table.NewName).FirstOrDefault() ?? tableName;
        }

        public bool TryGetNewName(string foreignKeyTableName, out string newName)
        {
            newName = null;

            foreach (var table in ParentTables)
            {
                if (table.Name != foreignKeyTableName)
                    continue;

                newName = table.NewName;
                return true;
            }

            return false;
        }
    }
}
