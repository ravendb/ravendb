using System;
using System.Collections.Generic;

namespace Raven.Server.SqlMigration
{
    public class RavenDocumentFactory
    {
        public WriteOptions Options { get;}

        public RavenDocumentFactory() : this(new WriteOptions()) {}

        public RavenDocumentFactory(WriteOptions options)
        {
            Options = options;
        }

        public RavenDocument FromReader(SqlReader reader, SqlTable table, bool valuesOnly = false)
        {
            return FromReader(reader, table, out _, valuesOnly);
        }

        public RavenDocument FromReader(SqlReader reader, SqlTable table, out Dictionary<string, byte[]> attachments, bool valuesOnly = false)
        {
            attachments = new Dictionary<string, byte[]>();
            var document = FromReaderInternal(reader, table, ref attachments, valuesOnly);
            return document;
        }

        private RavenDocument FromReaderInternal(SqlReader reader, SqlTable table, ref Dictionary<string, byte[]> attachments, bool valuesOnly = false)
        {
            var document = new RavenDocument(table.Name);

            var id = GetName(table.Name);

            if (!table.IsEmbedded)
                document.SetCollection(id);

            for (var i = 0; i < reader.FieldCount; i++)
            {
                var columnName = reader.GetName(i);

                object value;

                try
                {
                    value = reader[i];
                }
                catch (Exception e)
                {
                    if (Options.SkipUnsopportedTypes)
                        continue;

                    throw new InvalidOperationException($"Cannot read column '{columnName}' in table '{table.Name}'. (Unsupported type: {reader.GetDataTypeName(i)})", e);
                }

                var isForeignKey = table.ForeignKeys.TryGetValue(columnName, out var foreignKeyTable);
                var isNullOrEmpty = value is DBNull || string.IsNullOrWhiteSpace(value.ToString());

                if (table.PrimaryKeys.Contains(columnName))
                {
                    id += $"/{value}";

                    if (!isForeignKey && !table.IsEmbedded)
                        continue;
                }

                if (Options.BinaryToAttachment && reader.GetFieldType(i) == typeof(byte[]))
                {
                    if (!isNullOrEmpty)
                        attachments.Add($"{columnName}_{attachments.Count}", (byte[]) value);
                }

                else
                {
                    if (isForeignKey && !isNullOrEmpty)
                        value = $"{GetName(foreignKeyTable)}/{value}";

                    document.Set(columnName, value, Options.TrimStrings);
                }
            }

            if (!valuesOnly)
            {
                SetEmbeddedDocuments(document, table, ref attachments);
                table.GetJsPatcher()?.PatchDocument(document);
            }
            
            document.Id = id;

            return document;
        }

        private void SetEmbeddedDocuments(RavenDocument document, SqlTable parentTable, ref Dictionary<string, byte[]> attachments)
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

                if (!childReader.HasValue() && !childReader.Read())
                    continue;

                var continueLoop = false;

                while (!CompareValues(parentValues, GetValuesFromColumns(childReader, childColumns), out var isBigger))
                {
                    if (!isBigger && childReader.Read()) continue;

                    continueLoop = true;
                    break;
                }

                if (continueLoop)
                    continue;

                do
                {
                    var innerDocument = FromReaderInternal(childReader, childTable, ref attachments);

                    document.Append(childTable.Property, innerDocument);

                    if (!childReader.Read()) break;

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
            return Options.IncludeSchema ? tableName : tableName.Substring(tableName.IndexOf('.') + 1);
        }

        public class WriteOptions
        {
            public bool IncludeSchema { get; set; } = true;
            public bool BinaryToAttachment { get; set; } = true;
            public bool TrimStrings { get; set; } = true;
            public bool SkipUnsopportedTypes { get; set; } = false;
        }
    }
}
