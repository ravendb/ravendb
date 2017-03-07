using System;
using System.Collections.Generic;
using System.Data;
using Jint;
using Jint.Native;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.SqlReplication
{
    internal class SqlReplicationPatchDocument : PatchDocument
    {
        private const int DefaultSize = 50;

        private readonly DocumentsOperationContext _context;
        private readonly SqlReplicationScriptResult _scriptResult;
        private readonly SqlReplicationConfiguration _config;
        private readonly string _documentKey;

        public SqlReplicationPatchDocument(DocumentDatabase database, DocumentsOperationContext context, SqlReplicationScriptResult scriptResult, SqlReplicationConfiguration config, string documentKey)
            : base(database)
        {
            _context = context;
            _scriptResult = scriptResult;
            _config = config;
            _documentKey = documentKey;
        }

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

            engine.SetValue("documentId", _documentKey);
            engine.SetValue("replicateTo", new Action<string, JsValue>((tableName, colsAsObject) => ReplicateToFunction(tableName, colsAsObject, scope)));
            _scriptResult.Keys.Add(_documentKey);
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

            var itemToReplicates = _scriptResult.Data.GetOrAdd(tableName);
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
            itemToReplicates.Add(new ItemToReplicate
            {
                DocumentKey = _documentKey,
                Columns = columns
            });
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