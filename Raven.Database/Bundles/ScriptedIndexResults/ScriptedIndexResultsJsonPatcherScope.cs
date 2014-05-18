using System;
using System.Collections.Generic;
using System.Linq;

using Jint;
using Jint.Native;

using Raven.Abstractions.Data;
using Raven.Database.Json;

namespace Raven.Database.Bundles.ScriptedIndexResults
{
	public class ScriptedIndexResultsJsonPatcherScope : DefaultScriptedJsonPatcherOperationScope
	{
		private readonly HashSet<string> entityNames;

		private readonly HashSet<string> forbiddenDocuments = new HashSet<string>();

		public ScriptedIndexResultsJsonPatcherScope(DocumentDatabase database, HashSet<string> entityNames)
			: base(database)
		{
			this.entityNames = entityNames;
		}

		public override JsValue LoadDocument(string documentKey, Engine engine)
		{
			var document =  base.LoadDocument(documentKey, engine);
			if (document != JsValue.Null && !forbiddenDocuments.Contains(documentKey))
			{
				Database.TransactionalStorage.Batch(accessor =>
				{
					var refs = accessor.Indexing.GetDocumentsReferencing(documentKey);
					if (entityNames.Any(entityName => refs.Any(referencedDocumentKey => referencedDocumentKey.StartsWith(entityName, StringComparison.OrdinalIgnoreCase))))
						forbiddenDocuments.Add(documentKey);
				});
			}	

			return document;
		}

		public override void PutDocument(string documentKey, object data, object meta, Engine engine)
		{
			if (forbiddenDocuments.Contains(documentKey))
				throw new InvalidOperationException(string.Format("Cannot PUT document '{0}' to prevent infinite indexing loop. Avoid modifying documents that could be indirectly referenced by index.", documentKey));

			base.PutDocument(documentKey, data, meta, engine);
		}

		public override void DeleteDocument(string documentKey)
		{
			DeleteFromContext(documentKey);
		}

		public override void Dispose()
		{
		}

		protected override void ValidateDocument(JsonDocument newDocument)
		{
			if (newDocument.Metadata == null)
				return;
			var entityName = newDocument.Metadata.Value<string>(Constants.RavenEntityName);
			if (string.IsNullOrEmpty(entityName))
			{
				if (entityNames.Count == 0)
					throw new InvalidOperationException(
						"Invalid Script Index Results Recursion!\r\n" +
						"The scripted index result doesn't have an entity name, but the index apply to all documents.\r\n" +
						"Scripted Index Results cannot create documents that will be indexed by the same document that created them, " +
						"since that would create a infinite loop of indexing/creating documents.");
				return;
			}
			if (entityNames.Contains(entityName))
			{
				throw new InvalidOperationException(
					"Invalid Script Index Results Recursion!\r\n" +
					"The scripted index result have an entity name of " + entityName + ", but the index apply to documents with that entity name.\r\n" +
					"Scripted Index Results cannot create documents that will be indexed by the same document that created them, " +
					"since that would create a infinite loop of indexing/creating documents.");
			}
		}
	}
}