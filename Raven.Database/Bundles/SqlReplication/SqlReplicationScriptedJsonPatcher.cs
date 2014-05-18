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
			jintEngine.Global.Delete("documentId", true);
			jintEngine.Global.Delete("replicateTo", true);
			foreach (var sqlReplicationTable in config.SqlReplicationTables)
			{
				jintEngine.Global.Delete("replicateTo" + sqlReplicationTable.TableName, true);
			}
		}

		protected override void CustomizeEngine(Engine jintEngine, ScriptedJsonPatcherOperationScope scope)
		{
			jintEngine.SetValue("documentId", docId);
			jintEngine.SetValue("replicateTo", new Action<string,object>(ReplicateToFunction));
			foreach (var sqlReplicationTable in config.SqlReplicationTables)
			{
				var current = sqlReplicationTable;
				jintEngine.SetValue("replicateTo" + sqlReplicationTable.TableName, (Action<object>)(cols =>
				{
					var tableName = current.TableName;
					ReplicateToFunction(tableName, cols);
				}));
			}
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
	}
}