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

            private void AddErrors(IEnumerable<string> messages)
            {
                _errors.AddRange(messages);
            }

            public void Validate(out List<string> errs)
            {
                _errors.Clear();

                var tablesToValidate = _database.Tables.ToList();

                ValidateExistanceOfTables(tablesToValidate);
                ValidateEmbeddedTables(tablesToValidate);
                ValidateDuplicateTables(tablesToValidate);

                foreach (var table in tablesToValidate)
                {
                    ValidateHasPrimaryKeys(table);

                    var reader = ValidateQuery(table);

                    if (reader == null)
                        continue;

                    var document = ValidateCanCreateDocument(table, reader);

                    if (document == null)
                        continue;

                    ValidateQueryContainsAllKeys(document, table);
                    ValidatePatch(table, document);
                }

                errs = _errors;

                IsValid = _errors.Count == 0;
            }

            private void ValidateExistanceOfTables(List<SqlTable> tables)
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

                    else if (allTables.Contains(table.Name) == false)
                    {
                        AddError($"Couldn't find table '{table.Name}' in the sql database (Table name must include schema name)");
                        tables.Remove(table);
                    }
                    else count++;
                }
            }

            private void ValidateEmbeddedTables(List<SqlTable> tables)
            {
                AddErrors(

                    from sqlTable in tables
                    from embeddedTable in sqlTable.EmbeddedTables

                    where embeddedTable.ForeignKeys.ContainsValue(sqlTable.Name) == false && tables.Contains(embeddedTable)

                    select $"Table '{embeddedTable.Name}' cannot embed into '{sqlTable.Name}'"

                );
            }

            private void ValidateDuplicateTables(List<SqlTable> tables)
            {

                AddErrors(tables.Where(table => !table.IsEmbedded).GroupBy(table =>
                    {
                        ValidateDuplicateEmbeddedTables(table.EmbeddedTables);
                        return table.Name;
                    })
                    .Where(g => g.Count() > 1).Select(y => $"Duplicate table '{y.Key}'"));
            }

            private void ValidateDuplicateEmbeddedTables(List<SqlEmbeddedTable> tableEmbeddedTables)
            {
                var properties = new List<string>();
                var tables = new List<SqlTable>();

                foreach (var embeddedTable in tableEmbeddedTables)
                {
                    if (properties.Contains(embeddedTable.PropertyName))
                        AddError($"Duplicate property name '{embeddedTable.PropertyName}'");

                    else
                        properties.Add(embeddedTable.PropertyName);

                    if (tables.Contains(embeddedTable))
                        AddError($"Duplicate table '{embeddedTable.Name}' (try give them property name)");

                    else
                        tables.Add(embeddedTable);

                    ValidateDuplicateEmbeddedTables(embeddedTable.EmbeddedTables);
                }
            }

            private void ValidatePatch(SqlTable table, SqlMigrationDocument document)
            {
                try
                {
                    table.GetJsPatch().PatchDocument(document);
                }
                catch (Exception e)
                {
                    AddError($"Cannot patch table '{table.Name}' using the given script. Error: " + e);
                }
            }

            private void ValidateHasPrimaryKeys(SqlTable table)
            {
                if (table.PrimaryKeys.Count == 0)
                    AddError($"Table '{table.Name}' must have at list 1 primary key");
            }

            private SqlReader ValidateQuery(SqlTable table)
            {

                if (table.InitialQuery.IndexOf("order by", StringComparison.OrdinalIgnoreCase) != -1)
                {
                    AddError($"Query cannot contain an 'ORDER BY' clause ({table.Name})");
                    return null;
                }

                var reader = new SqlReader(_database.Connection, SqlQueries.SelectSingleRowFromQuery(table.InitialQuery));

                try
                {
                    reader.ExecuteReader();
                }
                catch
                {
                    AddError($"Failed to read table '{table.Name}' using the given query");
                    return null;
                }

                return reader;
            }

            private SqlMigrationDocument ValidateCanCreateDocument(SqlTable table, SqlReader reader)
            {
                using (reader)
                {
                    while (reader.Read())
                    {
                        try
                        {
                            return  _database.Factory.FromReader(reader, table, out _, true);
                        }
                        catch (Exception e)
                        {
                            AddError(e.Message);
                        }
                    }
                }
                return null;
            }

            private void ValidateQueryContainsAllKeys(SqlMigrationDocument document, SqlTable table)
            {
                var id = document.Id;

                var count = id.Count(c => c == '/');

                if (count < table.PrimaryKeys.Count)
                    AddError($"Query for table '{table.Name}' must select all primary keys");

                if (!(table is SqlEmbeddedTable embeddedTable)) return;

                if (embeddedTable.GetColumnsReferencingParentTable().Any(column => document.Properties.All(property => property.Name != column)))
                    AddError($"Query for table '{embeddedTable.Name}' must select all referential keys");
            }
        }
    }
}
