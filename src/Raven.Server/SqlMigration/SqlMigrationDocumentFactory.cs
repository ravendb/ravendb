using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.Migration;

namespace Raven.Server.SqlMigration
{
    public class SqlMigrationDocumentFactory
    {
        public FactoryOptions Options { get; }
        private readonly Dictionary<string, byte[]> _currentAttachments;

        public SqlMigrationDocumentFactory(FactoryOptions options)
        {
            Options = options;
            _currentAttachments = new Dictionary<string, byte[]>();
        }

        public SqlMigrationDocument FromReader(SqlReader reader, SqlTable table, out Dictionary<string, byte[]> attachments, SqlDatabase.Validator validator = null)
        {
            _currentAttachments?.Clear();
            var document = FromReaderInternal(reader, table, validator);
            attachments = _currentAttachments.ToDictionary(entry => entry.Key, entry => entry.Value);
            return document;
        }

        private SqlMigrationDocument FromReaderInternal(SqlReader reader, SqlTable table, SqlDatabase.Validator validator = null)
        {
            var document = new SqlMigrationDocument(table.Name);

            var id = table.NewName;

            if (table.IsEmbedded == false)
                document.SetCollection(id);

            for (var i = 0; i < reader.FieldCount; i++)
            {
                var columnName = reader.GetName(i);
                var isPrimaryKey = table.PrimaryKeys.Contains(columnName);
                var isForeignKey = table.ForeignKeys.TryGetValue(columnName, out var foreignKeyTableName);

                object value;

                try
                {
                    value = reader[i];
                }
                catch (Exception e)
                {
                    if (!(e is PlatformNotSupportedException))
                        throw;

                    var isKey = isPrimaryKey || isForeignKey;

                    if (Options.SkipUnsupportedTypes == false)
                    {
                        var message = $"Cannot read column '{columnName}' in table '{table.Name}'. (Unsupported type: {reader.GetDataTypeName(i)}) Error: {e}";

                        if (validator != null)
                            validator.AddError(SqlMigrationImportResult.Error.ErrorType.UnsupportedType, message, table.Name, columnName);

                        else
                            throw new InvalidOperationException(message, e);
                    }

                    else if (isKey)
                    {
                        var message = $"Cannot skip unsupported KEY column '{columnName}' in table '{table.Name}'. (Unsupported type: {reader.GetDataTypeName(i)})";

                        if (validator != null)
                            validator.AddError(SqlMigrationImportResult.Error.ErrorType.UnsupportedType, message, table.Name, columnName);
                        else
                            throw new InvalidOperationException(message, e);
                    }

                    continue;
                }

                if (isPrimaryKey)
                {
                    id += $"/{value}";

                    if (isForeignKey == false && table.IsEmbedded == false)
                        continue;
                }

                var isNullOrEmpty = value is DBNull || string.IsNullOrWhiteSpace(value.ToString());

                if (Options.BinaryToAttachment && reader.GetFieldType(i) == typeof(byte[]))
                {
                    if (isNullOrEmpty == false)
                        _currentAttachments.Add($"{columnName}_{_currentAttachments.Count}", (byte[])value);
                }

                else
                {
                    if (isForeignKey && isNullOrEmpty == false && table.Database.TryGetNewName(foreignKeyTableName, out var newName))
                        value = $"{newName}/{value}";

                    document.Set(columnName, value, Options.TrimStrings);
                }
            }

            document.Id = id;

            if (validator != null)
                return document;

            SetEmbeddedDocuments(reader, document, table);
            return document;
        }

        private void SetEmbeddedDocuments(SqlReader reader, SqlMigrationDocument document, SqlTable parentTable)
        {

            foreach (var childTable in parentTable.EmbeddedTables)
            {
                var parentValues = GetValuesFromColumns(reader, parentTable.PrimaryKeys); // values of referenced columns

                var childColumns = childTable.GetColumnsReferencingParentTable(); // values of referencing columns

                using (var childReader = GetChildReader(parentTable, childColumns, childTable, parentValues))
                {
                    if (childReader.HasValue() == false && childReader.Read() == false)
                        continue;

                    var continueLoop = false;

                    while (CompareValues(parentValues, GetValuesFromColumns(childReader, childColumns), out var isBigger) == false)
                    {
                        if (isBigger == false && childReader.Read())
                            continue;

                        continueLoop = true; // If parent value is greater than child value => childReader move to next. Otherwise => parentReader move to next
                        break;
                    }

                    if (continueLoop)
                        continue;

                    do
                    {
                        var innerDocument = FromReaderInternal(childReader, childTable);

                        document.Append(childTable.NewName, innerDocument);

                        if (childReader.Read() == false)
                            break;
                       

                        

                    } while (CompareValues(parentValues, GetValuesFromColumns(childReader, childColumns), out _));
                }
            }

        }

        private static SqlReader GetChildReader(SqlTable parentTable, List<string> childColumns, SqlEmbeddedTable childTable, List<string> parentValues)
        {

            if (childColumns.Count > parentTable.PrimaryKeys.Count || parentTable.IsEmbedded)
                // This happens in a case when we can not iterate the embedded table only once and have to use multiple queries.
                return childTable.GetReaderWhere(parentValues);

            return childTable.GetReader();
        }

        private static List<string> GetValuesFromColumns(SqlReader reader, List<string> childColumnName)
        {
            var lst = new List<string>();

            foreach (var columnName in childColumnName)
                lst.Add(reader[columnName].ToString());

            return lst;
        }

        private static bool CompareValues(List<string> parentValues, List<string> childValues, out bool continueLoop)
        {
            continueLoop = false;

            for (var i = 0; i < childValues.Count / parentValues.Count; i++)
            {
                var equal = true;

                for (var j = 0; j < parentValues.Count; j++)
                {
                    if (parentValues[j] == childValues[i * parentValues.Count + j])
                        continue;

                    if (IsSmallerThan(parentValues[j], childValues[i * parentValues.Count + j]))
                        continueLoop = true;

                    equal = false;
                    break;
                }
                if (equal)
                    return true;
            }
            return false;
        }

        private static bool IsSmallerThan(string parentValue, string childValue)
        {
            if (double.TryParse(childValue, out var d))
                return d > double.Parse(parentValue);

            return
                string.Compare(childValue, parentValue, StringComparison.Ordinal) > 0;
        }

        public class FactoryOptions
        {
            public bool BinaryToAttachment { get; set; } = true;
            public bool TrimStrings { get; set; } = true;
            public bool SkipUnsupportedTypes { get; set; } = false;
            public int BatchSize { get; set; } = 1000;
        }
    }
}
