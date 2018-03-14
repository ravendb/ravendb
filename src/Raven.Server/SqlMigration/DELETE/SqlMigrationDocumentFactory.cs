using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.SqlMigration
{
    /* TODO
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
            var document =  FromReaderInternal(reader, table, validator);
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
                        _currentAttachments.Add($"{columnName}_{_currentAttachments.Count}", (byte[]) value);
                }

                else
                {
                    if (isForeignKey && isNullOrEmpty == false && table.Database.TryGetNewName(foreignKeyTableName, out var newName))
                        value = $"{newName}/{value}";

                    document.Set(columnName, value, Options.TrimStrings);
                }
            }

            document.Id = id;

            if (validator == null) 
                SetEmbeddedDocuments(document, table);

            return document;
        }

       

       

    }*/
}
