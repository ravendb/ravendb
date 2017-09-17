using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.SqlMigration
{

    public class SqlMigrationDocumentFactory
    {
        public FactoryOptions Options { get; }
        private readonly Dictionary<string, byte[]> _currentAttachments = new Dictionary<string, byte[]>();
        public HashSet<string> ColumnsSkipped;

        public SqlMigrationDocumentFactory(FactoryOptions options)
        {
            Options = options;
            ColumnsSkipped = new HashSet<string>();
        }

        public SqlMigrationDocument FromReader(SqlReader reader, SqlTable table, out Dictionary<string, byte[]> attachments, bool toValidate = false)
        {
            _currentAttachments?.Clear();
            var document =  FromReaderInternal(reader, table, toValidate);
            attachments = _currentAttachments.ToDictionary(entry => entry.Key, entry => entry.Value);
            return document;
        }

        private SqlMigrationDocument FromReaderInternal(SqlReader reader, SqlTable table, bool toValidate = false)
        {
            var document = new SqlMigrationDocument(table.Name);

            var id = GetName(table.Name);

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
                    if (Options.SkipUnsupportedTypes == false)
                        throw new InvalidOperationException($"Cannot read column '{columnName}' in table '{table.Name}'. (Unsupported type: {reader.GetDataTypeName(i)})", e);

                    if (isPrimaryKey || isForeignKey)
                        throw new InvalidOperationException($"Cannot skip unsupported KEY column '{columnName}' in table '{table.Name}'. (Unsupported type: {reader.GetDataTypeName(i)})", e);

                    if (!toValidate)
                        ColumnsSkipped.Add($"{table.Name}: {columnName}");

                    continue;
                }

                var isNullOrEmpty = value is DBNull || string.IsNullOrWhiteSpace(value.ToString());

                if (isPrimaryKey)
                {
                    id += $"/{value}";

                    if (isForeignKey == false && table.IsEmbedded == false)
                        continue;
                }

                if (Options.BinaryToAttachment && reader.GetFieldType(i) == typeof(byte[]))
                {
                    if (isNullOrEmpty == false)
                        _currentAttachments.Add($"{columnName}_{_currentAttachments.Count}", (byte[]) value);
                }

                else
                {
                    if (isForeignKey && isNullOrEmpty == false)
                        value = $"{GetName(foreignKeyTableName)}/{value}";

                    document.Set(columnName, value, Options.TrimStrings);
                }
            }

            document.Id = id;

            if (toValidate) return document;

            SetEmbeddedDocuments(document, table);
            table.GetJsPatch().PatchDocument(document);

            return document;
        }

        private void SetEmbeddedDocuments(SqlMigrationDocument document, SqlTable parentTable)
        {
            foreach (var childTable in parentTable.EmbeddedTables)
            {
                var parentValues = GetValuesFromColumns(parentTable.GetReader(), parentTable.PrimaryKeys);

                var childColumns = childTable.GetColumnsReferencingParentTable();

                SqlReader childReader;

                if (childColumns.Count > parentTable.PrimaryKeys.Count || parentTable.IsEmbedded)
                    childReader = childTable.GetReaderWhere(parentValues);

                else
                    childReader = childTable.GetReader();

                if (childReader.HasValue() == false && childReader.Read() == false)
                    continue;

                var continueLoop = false;

                while (CompareValues(parentValues, GetValuesFromColumns(childReader, childColumns), out var isBigger) == false)
                {
                    if (isBigger == false && childReader.Read()) continue;

                    continueLoop = true; // If parent value is greater than child value => childReader move to next, otherwise, parentReader move to next
                    break;
                }

                if (continueLoop)
                    continue;

                do
                {
                    var innerDocument = FromReaderInternal(childReader, childTable);

                    document.Append(childTable.PropertyName, innerDocument);

                    if (childReader.Read() == false) break;

                } while (CompareValues(parentValues, GetValuesFromColumns(childReader, childColumns), out _));
            }
        }

        private List<string> GetValuesFromColumns(SqlReader reader, List<string> childColumnName)
        {
            var lst = new List<string>();

            foreach (var columnName in childColumnName)
                    lst.Add(reader[columnName].ToString());

            return lst;
        }

        private bool CompareValues(List<string> parentValues, List<string> childValues, out bool continueLoop)
        {
            continueLoop = false;

            for (var i = 0; i < childValues.Count / parentValues.Count; i++)
            {
                var equal = true;

                for (var j = 0; j < parentValues.Count; j++)
                {
                    if (parentValues[j] == childValues[i * parentValues.Count + j]) continue;

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

        private bool IsSmallerThan(string parentValue, string childValue)
        {
            if (double.TryParse(childValue, out var d))
                return d > double.Parse(parentValue);

            return
                string.Compare(childValue, parentValue, StringComparison.Ordinal) > 0;
        }

        private string GetName(string tableName)
        {
            return Options.IncludeSchema ? tableName : tableName.Substring(tableName.LastIndexOf('.') + 1);
        }

        public class FactoryOptions
        {
            public bool IncludeSchema { get; set; } = true;
            public bool BinaryToAttachment { get; set; } = true;
            public bool TrimStrings { get; set; } = true;
            public bool SkipUnsupportedTypes { get; set; } = false;
            public int BatchSize { get; set; } = 1000;
        }
    }
}
