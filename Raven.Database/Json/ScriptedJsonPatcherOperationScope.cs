// -----------------------------------------------------------------------
//  <copyright file="ScriptedJsonPatcherOperationScope.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;

using Jint;
using Jint.Native;

using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Json
{
    public abstract class ScriptedJsonPatcherOperationScope : JintOperationScope
	{
		protected DocumentDatabase Database { get; private set; }

		protected ScriptedJsonPatcherOperationScope(DocumentDatabase database)
		{
			Database = database;
		}

		protected virtual void ValidateDocument(JsonDocument newDocument)
		{
		}

		public abstract JsValue LoadDocument(string documentKey, Engine engine);

		public abstract void PutDocument(string documentKey, object data, object meta, Engine jintEngine);

		public abstract void DeleteDocument(string documentKey);
	}

	public class DefaultScriptedJsonPatcherOperationScope : ScriptedJsonPatcherOperationScope
	{
		private readonly Dictionary<string, JsonDocument> documentKeyContext = new Dictionary<string, JsonDocument>();
		private readonly List<JsonDocument> incompleteDocumentKeyContext = new List<JsonDocument>();

		private static readonly string[] EtagKeyNames = {
			                                                "etag",
			                                                "@etag",
			                                                "Etag",
			                                                "ETag"
		                                                };

		public DefaultScriptedJsonPatcherOperationScope(DocumentDatabase database = null)
			: base(database)
		{
		}

		public override JsValue LoadDocument(string documentKey, Engine engine)
		{
			if (Database == null)
				throw new InvalidOperationException("Cannot load by id without database context");

			JsonDocument document;
			if (documentKeyContext.TryGetValue(documentKey, out document) == false)
				document = Database.Documents.Get(documentKey, null);

			var loadedDoc = document == null ? null : document.ToJson();

			if (loadedDoc == null)
				return JsValue.Null;

			loadedDoc[Constants.DocumentIdFieldName] = documentKey;
			return ToJsObject(engine, loadedDoc);
		}

		public override void PutDocument(string key, object documentAsObject, object metadataAsObject, Engine engine)
		{
			if (documentAsObject == null)
			{
				throw new InvalidOperationException(
					string.Format("Created document cannot be null or empty. Document key: '{0}'", key));
			}

			var newDocument = new JsonDocument
			{
				Key = key,
				//DataAsJson = ToRavenJObject(doc)
				DataAsJson = RavenJObject.FromObject(documentAsObject)
			};

			if (metadataAsObject == null)
			{
				RavenJToken value;
				if (newDocument.DataAsJson.TryGetValue("@metadata", out value))
				{
					newDocument.DataAsJson.Remove("@metadata");
					newDocument.Metadata = (RavenJObject)value;
				}
			}
			else
			{
				var metadata = RavenJObject.FromObject(metadataAsObject);

				foreach (var etagKeyName in EtagKeyNames)
				{
					RavenJToken etagValue;
					if (!metadata.TryGetValue(etagKeyName, out etagValue))
						continue;

					metadata.Remove(etagKeyName);

					var etag = etagValue.Value<string>();
					if (string.IsNullOrEmpty(etag))
						continue;

					Etag newDocumentEtag;
					if (Etag.TryParse(etag, out newDocumentEtag) == false)
						throw new InvalidOperationException(string.Format("Invalid ETag value '{0}' for document '{1}'", etag, key));

					newDocument.Etag = newDocumentEtag;
				}

				newDocument.Metadata = metadata;
			}

			ValidateDocument(newDocument);
			AddToContext(key, newDocument);
		}

		public override void DeleteDocument(string documentKey)
		{
			throw new NotSupportedException("Deleting documents is not supported.");
		}

		public override void Dispose()
		{
		}

		protected void AddToContext(string key, JsonDocument document)
		{
			if (string.IsNullOrEmpty(key) || key.EndsWith("/"))
				incompleteDocumentKeyContext.Add(document);
			else
				documentKeyContext[key] = document;
		}

		protected void DeleteFromContext(string key)
		{
			documentKeyContext[key] = null;
		}

		public IEnumerable<ScriptedJsonPatcher.Operation> GetOperations()
		{
			return documentKeyContext.Select(x => new ScriptedJsonPatcher.Operation
			{
				Type = x.Value != null ? ScriptedJsonPatcher.OperationType.Put : ScriptedJsonPatcher.OperationType.Delete,
				DocumentKey = x.Key,
				Document = x.Value
			}).Union(incompleteDocumentKeyContext.Select(x => new ScriptedJsonPatcher.Operation
			{
				Type = ScriptedJsonPatcher.OperationType.Put,
				DocumentKey = x.Key,
				Document = x
			}));
		}

		public IEnumerable<JsonDocument> GetPutOperations()
		{
			return GetOperations().Where(x => x.Type == ScriptedJsonPatcher.OperationType.Put).Select(x => x.Document);
		}
	}
}