using System;
using Jint;
using Jint.Native;
using Raven.Abstractions.Extensions;
using Raven.Database.Json;

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

		protected override void RemoveEngineCustomizations(JintEngine jintEngine)
		{
			jintEngine.RemoveParameter("documentId");
			jintEngine.RemoveParameter("replicateTo");
			foreach (var sqlReplicationTable in config.SqlReplicationTables)
			{
				jintEngine.RemoveParameter("replicateTo" + sqlReplicationTable.TableName);
			}
		}

		protected override void CustomizeEngine(JintEngine jintEngine, ScriptedJsonPatcherOperationScope scope)
		{
			jintEngine.SetParameter("documentId", docId);
			jintEngine.SetFunction("replicateTo", new Action<string,JsObject>((tableName, cols) => ReplicateToFunction(tableName, cols, scope)));
			foreach (var sqlReplicationTable in config.SqlReplicationTables)
			{
				var current = sqlReplicationTable;
				jintEngine.SetFunction("replicateTo" + sqlReplicationTable.TableName, (Action<JsObject>)(cols =>
				{
					var tableName = current.TableName;
					ReplicateToFunction(tableName, cols, scope);
				}));
			}
		}

		private void ReplicateToFunction(string tableName, JsObject cols, ScriptedJsonPatcherOperationScope scope)
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