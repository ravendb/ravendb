using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using Jint;
using Jint.Native;
using Raven.Client.Documents.Attachments;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    internal class SqlDocumentTransformer : EtlTransformer<ToSqlItem, SqlTableWithRecords>
    {
        private const int DefaultSize = 50;

        private const string AttachmentMarker = "$attachment/";

        private readonly Transformation _transformation;
        private readonly SqlDestination _config;
        private readonly PatchRequest _patchRequest;
        private readonly Dictionary<string, SqlTableWithRecords> _tables;

        public SqlDocumentTransformer(Transformation transformation, DocumentDatabase database, DocumentsOperationContext context, SqlDestination config)
            : base(database, context)
        {
            _transformation = transformation;
            _config = config;
            _patchRequest = new PatchRequest { Script = transformation.Script };
            _tables = new Dictionary<string, SqlTableWithRecords>(_config.SqlTables.Count);
            
            var tables = new string[config.SqlTables.Count];

            for (var i = 0; i < config.SqlTables.Count; i++)
            {
                tables[i] = config.SqlTables[i].TableName;
            }

            LoadToDestinations = tables;
        }

        protected override string[] LoadToDestinations { get; }

        protected override void RemoveEngineCustomizations(Engine engine, PatcherOperationScope scope)
        {
            base.RemoveEngineCustomizations(engine, scope);
            
            engine.Global.Delete("varchar", true);
            engine.Global.Delete("nVarchar", true);
            engine.Global.Delete(LoadAttachment, true);
        }
        
        protected override void CustomizeEngine(Engine engine, PatcherOperationScope scope)
        {
            base.CustomizeEngine(engine, scope);

            engine.SetValue("varchar", (Func<string, double?, ValueTypeLengthTriple>)(ToVarchar));
            engine.SetValue("nVarchar", (Func<string, double?, ValueTypeLengthTriple>)(ToNVarchar));
            engine.SetValue(LoadAttachment, (Func<string, string>)(LoadAttachmentFunction));
        }

        protected override void LoadToFunction(string tableName, JsValue cols, PatcherOperationScope scope)
        {
            if (tableName == null)
                ThrowLoadParameterIsMandatory(nameof(tableName));
            if (cols == null)
                ThrowLoadParameterIsMandatory(nameof(cols));
            
            var dynamicJsonValue = scope.ToBlittable(cols.AsObject());
            var blittableJsonReaderObject = Context.ReadObject(dynamicJsonValue, tableName);
            var columns = new List<SqlColumn>(blittableJsonReaderObject.Count);
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (var i = 0; i < blittableJsonReaderObject.Count; i++)
            {
                blittableJsonReaderObject.GetPropertyByIndex(i, ref prop);

                var sqlColumn = new SqlColumn
                {
                    Id = prop.Name,
                    Value = prop.Value,
                    Type = prop.Token
                };

                if (_transformation.HasLoadAttachment && prop.Token == BlittableJsonToken.String && IsLoadAttachment(prop.Value as LazyStringValue, out var attachmentName))
                {
                    var attachmentStream = Database.DocumentsStorage.AttachmentsStorage.GetAttachment(
                                                   Context,
                                                   Current.DocumentId,
                                                   attachmentName,
                                                   AttachmentType.Document,
                                                   Current.Document.ChangeVector)
                                               ?.Stream ?? Stream.Null;

                    sqlColumn.Type = 0;
                    sqlColumn.Value = attachmentStream;
                }

                columns.Add(sqlColumn);
            }
            
            GetOrAdd(tableName).Inserts.Add(new ToSqlItem(Current)
            {
                Columns = columns
            });
        }

        private static unsafe bool IsLoadAttachment(LazyStringValue value, out string attachmentName)
        {
            if (value.Length <= AttachmentMarker.Length)
            {
                attachmentName = null;
                return false;
            }

            var buffer = value.Buffer;

            if (*(long*)buffer != 7883660417928814884 || // $attachm
                *(int*)(buffer + 8) != 796159589) // ent/
            {
                attachmentName = null;
                return false;
            }

            attachmentName = value.Substring(AttachmentMarker.Length);

            return true;
        }

        private string LoadAttachmentFunction(string attachmentName)
        {
            return $"{AttachmentMarker}{attachmentName}";
        }

        private SqlTableWithRecords GetOrAdd(string tableName)
        {
            SqlTableWithRecords table;
            if (_tables.TryGetValue(tableName, out table) == false)
            {
                _tables[tableName] =
                    table = new SqlTableWithRecords(_config.SqlTables.Find(x => x.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase)));
            }

            return table;
        }

        private ValueTypeLengthTriple ToVarchar(string value, double? sizeAsDouble)
        {
            return new ValueTypeLengthTriple
            {
                Type = DbType.AnsiString,
                Value = value,
                Size = sizeAsDouble.HasValue ? (int)sizeAsDouble.Value : DefaultSize
            };
        }

        private ValueTypeLengthTriple ToNVarchar(string value, double? sizeAsDouble)
        {
            return new ValueTypeLengthTriple
            {
                Type = DbType.String,
                Value = value,
                Size = sizeAsDouble.HasValue ? (int)sizeAsDouble.Value : DefaultSize
            };
        }

        public override IEnumerable<SqlTableWithRecords> GetTransformedResults()
        {
            return _tables.Values;
        }
         
        public override void Transform(ToSqlItem item)
        {
            if (item.IsDelete == false)
            {
                Current = item;

                Apply(Context, Current.Document, _patchRequest);
            }

            // ReSharper disable once ForCanBeConvertedToForeach
            for (int i = 0; i < _config.SqlTables.Count; i++)
            {
                // delete all the rows that might already exist there

                var sqlTable = _config.SqlTables[i];

                if (sqlTable.InsertOnlyMode)
                    continue;

                GetOrAdd(sqlTable.TableName).Deletes.Add(item);
            }
        }

        public class ValueTypeLengthTriple
        {
            public DbType Type { get; set; }
            public object Value { get; set; }
            public int Size { get; set; }
        }
    }
}