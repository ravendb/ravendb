using System;
using System.Collections.Generic;

namespace Raven.Server.SqlMigration
{
    public class RavenDocumentFactory
    {
        private readonly Options _options;

        public RavenDocumentFactory() : this(new Options()) {}

        public RavenDocumentFactory(Options options)
        {
            _options = options;
        }

        public RavenDocument FromReader(SqlReader reader, SqlTable table, bool valuesOnly = false)
        {
            return FromReader(reader, table, out _, valuesOnly);
        }

        public RavenDocument FromReader(SqlReader reader, SqlTable table, out Dictionary<string, byte[]> attachments, bool valuesOnly = false)
        {
            IDictionary<string, byte[]> attachmentsDic = new Dictionary<string, byte[]>();
            var document = FromReader(reader, table, ref attachmentsDic, valuesOnly);
            attachments = (Dictionary<string, byte[]>) attachmentsDic;
            return document;
        }

        private RavenDocument FromReader(SqlReader reader, SqlTable table, ref IDictionary<string, byte[]> attachments, bool valuesOnly = false)
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
                    if (_options.SkipUnsopportedTypes)
                        continue;

                    throw new InvalidOperationException($"Cannot read column '{columnName}' in table '{table.Name}'. (Unsupported type: {reader.GetDataTypeName(i)})", e);
                }

                table.ForeignKeys.TryGetValue(columnName, out var tableName);

                var isNullOrEmpty = value is DBNull || string.IsNullOrWhiteSpace(value.ToString());

                if (table.PrimaryKeys.Contains(columnName))
                {
                    id += $"/{value}";

                    if (tableName == null && !table.IsEmbedded)
                        continue;
                }

                if (_options.BinaryToAttachment && reader.GetFeildType(i) == typeof(byte[]))
                {
                    if (!isNullOrEmpty)
                        attachments.Add($"{columnName}_{attachments.Count}", (byte[]) value);
                }

                else
                {
                    if (tableName != null && !isNullOrEmpty)
                        value = $"{GetName(tableName)}/{value}";

                    document.Set(columnName, value, _options.TrimStrings);
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

        private void SetEmbeddedDocuments(RavenDocument document, SqlTable parentTable, ref IDictionary<string, byte[]> attachments)
        {
            foreach (var item in parentTable.EmbeddedTables)
            {
                var childTable = item.Item2;

                var childColumns = childTable.GetColumnsReferencingTable(parentTable.Name);

                var parentValues = GetValuesFromColumns(parentTable.GetReader(), parentTable.PrimaryKeys);

                SqlReader childReader;

                if (childColumns.Count > parentTable.PrimaryKeys.Count || parentTable.IsEmbedded)
                    childReader = childTable.GetReaderWhere(childColumns, parentValues);

                else
                    childReader = childTable.GetReader(childColumns);

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
                    var innerDocument = FromReader(childReader, childTable, ref attachments);

                    document.Append(item.Item1, innerDocument);

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
            return _options.IncludeSchema ? tableName : tableName.Substring(tableName.IndexOf('.') + 1);
        }

        public class Options
        {
            public bool IncludeSchema { get; set; } = true;
            public bool BinaryToAttachment { get; set; } = true;
            public bool TrimStrings { get; set; } = true;
            public bool SkipUnsopportedTypes { get; set; } = false;
        }
    }
}
