using System;
using System.Data;
using System.Diagnostics;
using Jint;
using Raven.Server.Documents.Patch;

namespace Raven.Server.Documents.SqlReplication
{
    internal class SqlReplicationPatchDocument : PatchDocument
    {
        private const int DefaultSize = 50;

        private readonly SqlReplicationScriptResult scriptResult;
        private readonly SqlReplicationConfiguration config;
        private readonly string _documentKey;

        public SqlReplicationPatchDocument(DocumentDatabase database,
                                                 SqlReplicationScriptResult scriptResult,
                                                 SqlReplicationConfiguration config,
                                                 string documentKey)
            : base(database)
        {
            this.scriptResult = scriptResult;
            this.config = config;
            this._documentKey = documentKey;
        }

        protected override void RemoveEngineCustomizations(Engine engine, PatcherOperationScope scope)
        {
            base.RemoveEngineCustomizations(engine, scope);

            engine.Global.Delete("documentId", true);
            engine.Global.Delete("replicateTo", true);
            engine.Global.Delete("varchar", true);
            engine.Global.Delete("nVarchar", true);
            foreach (var sqlReplicationTable in config.SqlReplicationTables)
            {
                engine.Global.Delete("replicateTo" + sqlReplicationTable.TableName, true);
            }
        }

        protected override void CustomizeEngine(Engine engine, PatcherOperationScope scope)
        {
            base.CustomizeEngine(engine, scope);

            engine.SetValue("documentId", _documentKey);
            engine.SetValue("replicateTo", new Action<string, object>(ReplicateToFunction));
            scriptResult.Keys.Add(_documentKey);
            foreach (var sqlReplicationTable in config.SqlReplicationTables)
            {
                var current = sqlReplicationTable;
                engine.SetValue("replicateTo" + sqlReplicationTable.TableName, (Action<object>)(cols =>
                {
                    var tableName = current.TableName;
                    ReplicateToFunction(tableName, cols);
                }));
            }

            engine.SetValue("varchar", (Func<string, double?, ValueTypeLengthTriple>)(ToVarchar));
            engine.SetValue("nVarchar", (Func<string, double?, ValueTypeLengthTriple>)(ToNVarchar));
        }

        private void ReplicateToFunction(string tableName, object colsAsObject)
        {
            if (tableName == null)
                throw new ArgumentException("tableName parameter is mandatory");
            if (colsAsObject == null)
                throw new ArgumentException("cols parameter is mandatory");

            var itemToReplicates = scriptResult.Data.GetOrAdd(tableName);
            Debugger.Break(); // TODO: see what we get here
            itemToReplicates.Add(new ItemToReplicate
            {
                DocumentKey = _documentKey,
                Columns = colsAsObject
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