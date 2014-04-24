using System;
using Jint;
using Jint.Native;
using Raven.Abstractions.Extensions;
using Raven.Database.Json;

namespace Raven.Database.Bundles.SqlReplication
{
	public class SqlReplicationScriptedJsonPatcher : ScriptedJsonPatcher
	{
		private readonly ScriptedJsonPatcherOperationScope scope;

		private readonly ConversionScriptResult scriptResult;
		private readonly SqlReplicationConfig config;
		private readonly string docId;

		public SqlReplicationScriptedJsonPatcher(DocumentDatabase database,
												 ScriptedJsonPatcherOperationScope scope,
		                                         ConversionScriptResult scriptResult,
												 SqlReplicationConfig config,
		                                         string docId)
			: base(database)
		{
			this.scope = scope;
			this.scriptResult = scriptResult;
			this.config = config;
			this.docId = docId;
		}

		protected override void RemoveEngineCustomizations(JintEngine jintEngine)
		{
			jintEngine.RemoveParameter("documentId");
			jintEngine.RemoveParameter("replicateTo");
			foreach (var sqlReplicationTable in config.SqlReplicationTables)
			{
				jintEngine.RemoveParameter("replicateTo" + sqlReplicationTable.TableName);
			}
		}

		protected override void CustomizeEngine(JintEngine jintEngine)
		{
			jintEngine.SetParameter("documentId", docId);
			jintEngine.SetFunction("replicateTo", new Action<string,JsObject>(ReplicateToFunction));
			foreach (var sqlReplicationTable in config.SqlReplicationTables)
			{
				var current = sqlReplicationTable;
				jintEngine.SetFunction("replicateTo" + sqlReplicationTable.TableName, (Action<JsObject>)(cols =>
				{
					var tableName = current.TableName;
					ReplicateToFunction(tableName, cols);
				}));
			}
		}

		private void ReplicateToFunction(string tableName, JsObject cols)
		{
			if (tableName == null)
				throw new ArgumentException("tableName parameter is mandatory");
			if (cols == null)
				throw new ArgumentException("cols parameter is mandatory");

			var itemToReplicates = scriptResult.Data.GetOrAdd(tableName);
			itemToReplicates.Add(new ItemToReplicate
			{
				DocumentId = docId,
				Columns = scope.ToRavenJObject(cols)
			});
		}
	}
}