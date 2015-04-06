using System;
using System.Data;
using System.Linq;
using Jint;
using Raven.Abstractions.Extensions;
using Raven.Database.Json;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.SqlReplication
{
	internal class SqlReplicationScriptedJsonPatcher : ScriptedJsonPatcher
	{
		private const int DefaultSize = 50;

		private readonly ConversionScriptResult scriptResult;
		private readonly SqlReplicationConfig config;
		private readonly string docId;

		public SqlReplicationScriptedJsonPatcher(DocumentDatabase database,
												 ConversionScriptResult scriptResult,
												 SqlReplicationConfig config,
												 string docId)
			: base(database)
		{
			this.scriptResult = scriptResult;
			this.config = config;
			this.docId = docId;
		}

		protected override void RemoveEngineCustomizations(Engine engine, ScriptedJsonPatcherOperationScope scope)
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

		protected override void CustomizeEngine(Engine engine, ScriptedJsonPatcherOperationScope scope)
		{
			base.CustomizeEngine(engine, scope);

			engine.SetValue("documentId", docId);
			engine.SetValue("replicateTo", new Action<string, object>(ReplicateToFunction));
            scriptResult.Ids.Add(docId);
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
			itemToReplicates.Add(new ItemToReplicate
			{
				DocumentId = docId,
				Columns = RavenJObject.FromObject(colsAsObject)
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