using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.Migration;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.SqlMigration
{
    public partial class SqlDatabase
    {
        public class Validator
        {
            public readonly List<SqlMigrationImportResult.Error> Errors;
            private readonly SqlDatabase _database;
            private readonly DocumentsOperationContext _context;
            public bool IsValid;

            public Validator(SqlDatabase database, DocumentsOperationContext context)
            {
                _database = database;
                _context = context;
                IsValid = false;
                Errors = new List<SqlMigrationImportResult.Error>();
            }

            public void AddError(SqlMigrationImportResult.Error.ErrorType errorType, string message, string tableName = null, string columnName = null)
            {
                Errors.Add(new SqlMigrationImportResult.Error
                {
                    Type = errorType,
                    Message = message,
                    TableName = tableName,
                    ColumnName = columnName
                });
            }

            private void AddErrors(IEnumerable<SqlMigrationImportResult.Error> messages)
            {
                Errors.AddRange(messages);
            }

            public void Validate(out List<SqlMigrationImportResult.Error> errs)
            {
                Errors.Clear();

                var tablesToValidate = _database.GetAllTables();

                ValidateExistanceOfTables(tablesToValidate);
                ValidateDuplicateTables(_database.ParentTables);
                ValidateDuplicateNames(tablesToValidate);
                ValidateEmbeddedTables(_database.EmbeddedTables.Intersect(tablesToValidate).ToList());

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

                    var doc = ValidateConvertableToBlittable(table, document);

                    if (doc == null)
                        continue;

                    if (table is SqlParentTable t)
                        ValidatePatch(t, doc);
                }

                errs = Errors;

                IsValid = Errors.Count == 0;
            }

            private BlittableJsonReaderObject ValidateConvertableToBlittable(SqlTable table, SqlMigrationDocument document)
            {
                try
                {
                    return document.ToBllitable(_context);
                }
                catch (Exception e)
                {
                    AddError(SqlMigrationImportResult.Error.ErrorType.ParseError, $"Cannot convert document to bllittable. Error: {e}", table.Name);
                    return null;
                }
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
                        AddError(SqlMigrationImportResult.Error.ErrorType.TableMissingName, "A table is missing a name");
                        tables.Remove(table);
                    }

                    else if (allTables.Contains(table.Name) == false)
                    {
                        AddError(SqlMigrationImportResult.Error.ErrorType.TableNotExist, $"Couldn't find table '{table.Name}' in the SQL database (Table name must include schema name)", table.Name);
                        tables.Remove(table);
                    }
                    else
                        count++;
                }
            }

            private void ValidateEmbeddedTables(List<SqlTable> embeddedTables)
            {
                foreach (var embeddedTable in embeddedTables)
                {
                    ValidateEmbeddedTables(new List<SqlTable>(embeddedTable.EmbeddedTables));
                    if (embeddedTable is SqlEmbeddedTable t && embeddedTable.ForeignKeys.ContainsValue(t.ParentTableName) == false)
                        AddError(SqlMigrationImportResult.Error.ErrorType.InvalidEmbed, $"Table '{embeddedTable.Name}' cannot embed into '{t.ParentTableName}'", embeddedTable.Name);
                }
            }

            private void ValidateDuplicateTables(List<SqlParentTable> tables)
            {
                AddErrors(tables.GroupBy(table => table.Name)
                    .Where(g => g.Count() > 1).Select(y => new SqlMigrationImportResult.Error
                    {
                        Type = SqlMigrationImportResult.Error.ErrorType.DuplicateParentTable,
                        Message = $"Duplicate parent table '{y.Key}'",
                        TableName = y.Key
                    }));
            }

            private void ValidateDuplicateNames(List<SqlTable> tables, bool first = true)
            {
                AddErrors(tables.Where(table =>
                    {
                        if (!first)
                            return true;
                        ValidateDuplicateNames(new List<SqlTable>(table.EmbeddedTables), false);
                        return table.IsEmbedded == false;
                    }).GroupBy(table => table.NewName)
                    .Where(g => g.Count() > 1).Select(y => new SqlMigrationImportResult.Error
                    {
                        Type = SqlMigrationImportResult.Error.ErrorType.DuplicateName,
                        Message = $"Duplicate name '{y.Key}'",
                        TableName = y.Key
                    }));
            }

            private void ValidatePatch(SqlParentTable table, BlittableJsonReaderObject document)
            {
                try
                {
                    table.GetJsPatch().PatchDocument(document);
                }
                catch (Exception e)
                {
                    AddError(SqlMigrationImportResult.Error.ErrorType.InvalidPatch, $"Cannot patch table '{table.Name}' using the given script. Error: {e}", table.Name);
                }
            }

            private void ValidateHasPrimaryKeys(SqlTable table)
            {
                if (table.PrimaryKeys.Count == 0)
                    AddError(SqlMigrationImportResult.Error.ErrorType.TableMissingPrimaryKeys, $"Table '{table.Name}' must have at list 1 primary key", table.Name);
            }

            private SqlReader ValidateQuery(SqlTable table)
            {

                if (table.InitialQuery.IndexOf("order by", StringComparison.OrdinalIgnoreCase) != -1)
                {
                    AddError(SqlMigrationImportResult.Error.ErrorType.InvalidOrderBy, $"Query cannot contain an 'ORDER BY' clause ({table.Name})", table.Name);
                    return null;
                }

                var reader = new SqlReader(_database.Connection, SqlQueries.SelectSingleRowFromQuery(table.InitialQuery));

                try
                {
                    reader.ExecuteReader();
                }
                catch (Exception e)
                {
                    AddError(SqlMigrationImportResult.Error.ErrorType.InvalidQuery, $"Failed to read table '{table.Name}' using the given query. Error: {e}");
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
                            return _database.Factory.FromReader(reader, table, out _, this);
                        }
                        catch (Exception e)
                        {
                            AddError(SqlMigrationImportResult.Error.ErrorType.UnsupportedType, e.Message, table.Name);
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
                    AddError(SqlMigrationImportResult.Error.ErrorType.InvalidQuery, $"Query for table '{table.Name}' must select all primary keys", table.Name);
            }
        }
    }
}
