using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using Jint;
using Jint.Native;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.SQL
{
    internal class SqlReplicationPatchDocument : PatchDocument
    {
        private const int DefaultSize = 50;

        private readonly DocumentsOperationContext _context;
        private readonly SqlReplicationConfiguration _config;
        private readonly PatchRequest _patchRequest;
        private ToSqlItem _current;

        public SqlReplicationPatchDocument(DocumentDatabase database, DocumentsOperationContext context, SqlReplicationConfiguration config)
            : base(database)
        {
            _context = context;
            _config = config;
            _patchRequest = new PatchRequest { Script = _config.Script };
            Tables = new Dictionary<string, SqlTableWithRecords>(_config.SqlReplicationTables.Count);
        }

        public readonly Dictionary<string, SqlTableWithRecords> Tables;

        protected override void RemoveEngineCustomizations(Engine engine, PatcherOperationScope scope)
        {
            base.RemoveEngineCustomizations(engine, scope);

            engine.Global.Delete("documentId", true);
            engine.Global.Delete("replicateTo", true);
            engine.Global.Delete("varchar", true);
            engine.Global.Delete("nVarchar", true);
            foreach (var sqlReplicationTable in _config.SqlReplicationTables)
            {
                engine.Global.Delete("replicateTo" + sqlReplicationTable.TableName, true);
            }
        }

        protected override void CustomizeEngine(Engine engine, PatcherOperationScope scope)
        {
            base.CustomizeEngine(engine, scope);

            Debug.Assert(_current != null);

            engine.SetValue("documentId", _current);
            engine.SetValue("replicateTo", new Action<string, JsValue>((tableName, colsAsObject) => ReplicateToFunction(tableName, colsAsObject, scope)));

            foreach (var sqlReplicationTable in _config.SqlReplicationTables)
            {
                var current = sqlReplicationTable;
                engine.SetValue("replicateTo" + sqlReplicationTable.TableName, (Action<JsValue>)(cols =>
                {
                    var tableName = current.TableName;
                    ReplicateToFunction(tableName, cols, scope);
                }));
            }

            engine.SetValue("varchar", (Func<string, double?, ValueTypeLengthTriple>)(ToVarchar));
            engine.SetValue("nVarchar", (Func<string, double?, ValueTypeLengthTriple>)(ToNVarchar));
        }

        private void ReplicateToFunction(string tableName, JsValue colsAsObject, PatcherOperationScope scope)
        {
            if (tableName == null)
                throw new ArgumentException("tableName parameter is mandatory");
            if (colsAsObject == null)
                throw new ArgumentException("cols parameter is mandatory");

            var dynamicJsonValue = scope.ToBlittable(colsAsObject.AsObject());
            var blittableJsonReaderObject = _context.ReadObject(dynamicJsonValue, tableName);
            var columns = new List<SqlReplicationColumn>(blittableJsonReaderObject.Count);
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (var i = 0; i < blittableJsonReaderObject.Count; i++)
            {
                blittableJsonReaderObject.GetPropertyByIndex(i, ref prop);
                columns.Add(new SqlReplicationColumn
                {
                    Key = prop.Name,
                    Value = prop.Value,
                    Type = prop.Token,
                });
            }
            
            GetOrAdd(tableName).Inserts.Add(new ToSqlItem(_current)
            {
                Columns = columns
            });
        }

        public SqlTableWithRecords GetOrAdd(string tableName)
        {
            SqlTableWithRecords table;
            if (Tables.TryGetValue(tableName, out table) == false)
            {
                Tables[tableName] =
                    table = new SqlTableWithRecords(_config.SqlReplicationTables.Find(x => x.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase)));
            }

            return table;
        }

        public void Transform(ToSqlItem item, DocumentsOperationContext context)
        {
            if (item.IsDelete)
            {
                // ReSharper disable once ForCanBeConvertedToForeach
                for (int i = 0; i < _config.SqlReplicationTables.Count; i++)
                {
                    GetOrAdd(_config.SqlReplicationTables[i].TableName).Deletes.Add(item);
                }

                return;
            }

            _current = item;

            Apply(context, _current.Document, _patchRequest);
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

        public class ValueTypeLengthTriple
        {
            public DbType Type { get; set; }
            public object Value { get; set; }
            public int Size { get; set; }
        }
    }
}