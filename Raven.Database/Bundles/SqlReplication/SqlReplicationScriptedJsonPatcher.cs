using System;
using Jint;
using Jint.Native;
using Raven.Database.Json;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.SqlReplication
{
	public class SqlReplicationScriptedJsonPatcher : ScriptedJsonPatcher
	{
		private readonly ConversionScriptResult scriptResult;
		private readonly string docId;

		public SqlReplicationScriptedJsonPatcher(DocumentDatabase database,
		                                         ConversionScriptResult scriptResult,
		                                         string docId)
			: base(database)
		{
			this.scriptResult = scriptResult;
			this.docId = docId;
		}

		protected override void RemoveEngineCustomizations(JintEngine jintEngine)
		{
			jintEngine.RemoveParameter("documentId");
			jintEngine.RemoveParameter("sqlReplicate");
		}

		protected override void CustomizeEngine(JintEngine jintEngine)
		{
			jintEngine.SetParameter("documentId", docId);
			jintEngine.SetFunction("sqlReplicate", (Action<string, string, JsObject>)((table, pkName, cols) =>
			{
				if (string.IsNullOrEmpty(table))
					throw new ArgumentException("table parameter is mandatory");
				if (string.IsNullOrEmpty(pkName))
					throw new ArgumentException("pkName parameter is mandatory");
				if (cols == null)
					throw new ArgumentException("cols parameter is mandatory");

				scriptResult.AddTable(table);
				var itemToReplicates = scriptResult.Data.GetOrAdd(table);
				itemToReplicates.Add(new ItemToReplicate
				{
					PkName = pkName,
					DocumentId = docId,
					Columns = ToRavenJObject(cols)
				});
			}));
		}

		protected override RavenJObject ConvertReturnValue(JsObject jsObject)
		{
			return null;// we don't use / need the return value
		}
	}
}