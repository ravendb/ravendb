using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.SqlMigration
{
    public partial class SqlDatabase
    {
        private class Validator
        {
            private readonly List<string> _errors = new List<string>();
            private readonly SqlDatabase _database;
            public bool IsValid;

            public Validator(SqlDatabase database)
            {
                _database = database;
                IsValid = false;
            }

            private void AddError(string message)
            {
                _errors.Add(message);
            }

            public void Validate(out List<string> errs)
            {
                _errors.Clear();

                var tableToValidate = _database.Tables.ToList();

                ValidateExistanceOfTables(ref tableToValidate);
                ValidateEmbeddedTables(tableToValidate);
                ValidateDuplicateTables(tableToValidate);

                foreach (var table in tableToValidate)
                {
                    ValidateHasPrimaryKeys(table);
                    
                    ValidateQuery(table, out var document);

                    if (document == null)
                        continue;

                    ValidatePatch(table, document);
                    ValidateQueryContainsAllPrimaryKeys(document.Id, table);
                }

                errs = _errors;

                IsValid = _errors.Count == 0;
            }

            private void ValidateExistanceOfTables(ref List<SqlTable> tables)
            {
                var allTables = GetAllTablesNamesFromDatabase(_database.Connection);

                var count = 0;

                while (count < tables.Count)
                {
                    var table = tables[count];

                    if (string.IsNullOrEmpty(table.Name))
                    {
                        AddError("A table is missing a name");
                        tables.Remove(table);
                    }

                    else if (!allTables.Contains(table.Name))
                    {
                        AddError($"Couldn't find table '{table.Name}' in the sql database (Table name must include schema name)");
                        tables.Remove(table);
                    }
                    else count++;
                }
            }

            private void ValidateEmbeddedTables(List<SqlTable> tables)
            {
                foreach (var table in tables)
                {
                    foreach (var embeddedTable in table.EmbeddedTables)
                    {
                        if (!embeddedTable.ForeignKeys.ContainsValue(table.Name) && tables.Contains(embeddedTable))
                            AddError($"Table '{embeddedTable.Name}' cannot embed into '{table.Name}'");
                    }
                }
            }

            private void ValidateDuplicateTables(List<SqlTable> tables)
            {
                var names = new List<string>();

                foreach (var table in tables)
                {
                    if (table.IsEmbedded)
                        continue;

                    if (names.Contains(table.Name))
                        AddError($"Duplicate table '{table.Name}'");

                    else 
                        names.Add(table.Name);

                    ValidateDuplicateEmbeddedTables(table.EmbeddedTables);
                }
            }

            private void ValidateDuplicateEmbeddedTables(List<SqlEmbeddedTable> tableEmbeddedTables)
            {
                var properties = new List<string>();
                var tables = new List<SqlTable>();

                foreach (var embeddedTable in tableEmbeddedTables)
                {
                    if (properties.Contains(embeddedTable.Property))
                        AddError($"Duplicate property name '{embeddedTable.Property}'");

                    else
                        properties.Add(embeddedTable.Property);

                    if (tables.Contains(embeddedTable))
                        AddError($"Duplicate table '{embeddedTable.Name}' (try give them property name)");

                    else
                        tables.Add(embeddedTable);

                    ValidateDuplicateEmbeddedTables(embeddedTable.EmbeddedTables);
                }
            }

            private void ValidatePatch(SqlTable table, RavenDocument document)
            {
                try
                {
                    table.GetJsPatcher()?.PatchDocument(document);
                }
                catch
                {
                    AddError($"Cannot patch table '{table.Name}' using the given script");
                }
            }

            private void ValidateHasPrimaryKeys(SqlTable table)
            {
                if (table.PrimaryKeys.Count == 0)
                    AddError($"Table '{table.Name}' must have at list 1 primary key");
            }

            private void ValidateQuery(SqlTable table, out RavenDocument document)
            {
                document = null;

                if (table.InitialQuery.ToLower().Contains(" order by "))
                    AddError($"Query cannot contain an 'ORDER BY' clause ({table.Name})");

                var reader = new SqlReader(_database.Connection, SqlQueries.SelectSingleRowFromQuery(table.InitialQuery));

                try
                {
                    reader.ExecuteReader();
                }
                catch
                {
                    AddError($"Failed to read table '{table.Name}' using the given query");
                    return;
                }

                using (reader)
                {
                    while (reader.Read())
                    {
                        try
                        {
                            document = _database.Factory.FromReader(reader, table, true);
                        }
                        catch (Exception e)
                        {
                            AddError(e.Message);
                        }
                    }
                }
            }

            private void ValidateQueryContainsAllPrimaryKeys(string id, SqlTable table)
            {
                var count = 0;
                foreach (var c in id)
                    if (c == '/')
                        count++;

                if (count < table.PrimaryKeys.Count)
                    AddError($"Query for table '{table.Name}' must select all primary keys");
            }
        }
    }
}
