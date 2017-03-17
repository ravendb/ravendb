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
    internal class SqlDocumentTransformer : EtlTransformer<ToSqlItem, SqlTableWithRecords>
    {
        private const int DefaultSize = 50;
        
        private readonly SqlEtlConfiguration _config;
        private readonly PatchRequest _patchRequest;
        private ToSqlItem _current;
        private readonly Dictionary<string, SqlTableWithRecords> _tables;

        public SqlDocumentTransformer(DocumentDatabase database, DocumentsOperationContext context, SqlEtlConfiguration config)
            : base(database, context)
        {
            _config = config;
            _patchRequest = new PatchRequest { Script = _config.Script };
            _tables = new Dictionary<string, SqlTableWithRecords>(_config.SqlTables.Count);
        }

        protected override void RemoveEngineCustomizations(Engine engine, PatcherOperationScope scope)
        {
            base.RemoveEngineCustomizations(engine, scope);

            engine.Global.Delete("documentId", true);
            engine.Global.Delete("replicateTo", true);
            engine.Global.Delete("varchar", true);
            engine.Global.Delete("nVarchar", true);
            foreach (var table in _config.SqlTables)
            {
                engine.Global.Delete("replicateTo" + table.TableName, true);
            }
        }

        protected override void CustomizeEngine(Engine engine, PatcherOperationScope scope)
        {
            base.CustomizeEngine(engine, scope);

            Debug.Assert(_current != null);

            engine.SetValue("documentId", _current);
            engine.SetValue("replicateTo", new Action<string, JsValue>((tableName, colsAsObject) => ReplicateToFunction(tableName, colsAsObject, scope)));

            foreach (var table in _config.SqlTables)
            {
                var current = table;
                engine.SetValue("replicateTo" + table.TableName, (Action<JsValue>)(cols =>
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
            var blittableJsonReaderObject = Context.ReadObject(dynamicJsonValue, tableName);
            var columns = new List<SqlColumn>(blittableJsonReaderObject.Count);
            var prop = new BlittableJsonReaderObject.PropertyDetails();

            for (var i = 0; i < blittableJsonReaderObject.Count; i++)
            {
                blittableJsonReaderObject.GetPropertyByIndex(i, ref prop);
                columns.Add(new SqlColumn
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
                _current = item;

                Apply(Context, _current.Document, _patchRequest);
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