using System;
using Jint;
using Jint.Native;
using Raven.Abstractions.Extensions;
using Raven.Database.Json;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.SqlReplication
{
	public class SqlReplicationScriptedJsonPatcher : ScriptedJsonPatcher
	{
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

		protected override void RemoveEngineCustomizations(Engine jintEngine)
		{
			//jintEngine.RemoveParameter("documentId");
			//jintEngine.RemoveParameter("replicateTo");
			//foreach (var sqlReplicationTable in config.SqlReplicationTables)
			//{
			//	jintEngine.RemoveParameter("replicateTo" + sqlReplicationTable.TableName);
			//}
		}

		protected override void CustomizeEngine(Engine jintEngine, ScriptedJsonPatcherOperationScope scope)
		{
			jintEngine.SetValue("documentId", docId);
			jintEngine.SetValue("replicateTo", new Action<string,JsValue>((tableName, cols) => ReplicateToFunction(tableName, cols, scope)));
			foreach (var sqlReplicationTable in config.SqlReplicationTables)
			{
				var current = sqlReplicationTable;
				jintEngine.SetValue("replicateTo" + sqlReplicationTable.TableName, (Action<JsValue>)(cols =>
				{
					var tableName = current.TableName;
					ReplicateToFunction(tableName, cols, scope);
				}));
			}
		}

		private void ReplicateToFunction(string tableName, JsValue cols, ScriptedJsonPatcherOperationScope scope)
		{
			if (tableName == null)
				throw new ArgumentException("tableName parameter is mandatory");
			if (cols == JsValue.Null)
				throw new ArgumentException("cols parameter is mandatory");

			var itemToReplicates = scriptResult.Data.GetOrAdd(tableName);
			itemToReplicates.Add(new ItemToReplicate
			{
				DocumentId = docId,
				Columns = cols.TryCast<RavenJObject>()
			});
		}
	}
}