//-----------------------------------------------------------------------
// <copyright file="DocumentRetriever.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.MEF;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Http;
using Raven.Abstractions.Json;
using System.Linq;

namespace Raven.Database.Impl
{
	public class DocumentRetriever : ITranslatorDatabaseAccessor
	{
		private readonly IDictionary<string, JsonDocument> cache = new Dictionary<string, JsonDocument>(StringComparer.InvariantCultureIgnoreCase);
		private readonly HashSet<string> loadedIdsForRetrieval = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly HashSet<string> loadedIdsForFilter = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly IStorageActionsAccessor actions;
		private readonly OrderedPartCollection<AbstractReadTrigger> triggers;

		public DocumentRetriever(IStorageActionsAccessor actions, OrderedPartCollection<AbstractReadTrigger> triggers)
		{
			this.actions = actions;
			this.triggers = triggers;
		}

		public JsonDocument RetrieveDocumentForQuery(IndexQueryResult queryResult, IndexDefinition indexDefinition, FieldsToFetch fieldsToFetch)
		{
			return ExecuteReadTriggers(ProcessReadVetoes(
				RetrieveDocumentInternal(queryResult, loadedIdsForRetrieval, fieldsToFetch, indexDefinition),
				null, ReadOperation.Query), null, ReadOperation.Query);
		}


		public JsonDocument ExecuteReadTriggers(JsonDocument document, TransactionInformation transactionInformation, ReadOperation operation)
		{
			return ExecuteReadTriggersOnRead(ProcessReadVetoes(document, transactionInformation, operation),
											 transactionInformation, operation);
		}

		private JsonDocument ExecuteReadTriggersOnRead(JsonDocument resultingDocument, TransactionInformation transactionInformation, ReadOperation operation)
		{
			if (resultingDocument == null)
				return null;

			triggers.Apply(
				trigger =>
				trigger.OnRead(resultingDocument.Key, resultingDocument.DataAsJson, resultingDocument.Metadata, operation,
				               transactionInformation));
		
			return resultingDocument;
		}

		private JsonDocument RetrieveDocumentInternal(
			IndexQueryResult queryResult,
			HashSet<string> loadedIds,
			FieldsToFetch fieldsToFetch, 
			IndexDefinition indexDefinition)
		{
			if (queryResult.Projection == null)
			{
				// duplicate document, filter it out
				if (loadedIds.Add(queryResult.Key) == false)
					return null;
				return GetDocumentWithCaching(queryResult.Key);
			}

			if (fieldsToFetch.IsProjection)
			{
				if (indexDefinition.IsMapReduce == false)
				{
					bool hasStoredFields = false;
					foreach (var fieldToFetch in fieldsToFetch)
					{
						FieldStorage value;
						if (indexDefinition.Stores.TryGetValue(fieldToFetch, out value) == false &&
							value != FieldStorage.No)
							continue;
						hasStoredFields = true;
					}
					if (hasStoredFields == false)
					{
						// duplicate document, filter it out
						if (loadedIds.Add(queryResult.Key) == false)
							return null;
					}
				}
				var fieldsToFetchFromDocument = fieldsToFetch.Where(fieldToFetch => queryResult.Projection.Property(fieldToFetch) == null);
				var doc = GetDocumentWithCaching(queryResult.Key);
				if (doc != null)
				{
					var result = doc.DataAsJson.SelectTokenWithRavenSyntax(fieldsToFetchFromDocument.ToArray());
					foreach (var property in result.Properties())
					{
						if(property.Value == null || property.Value.Type == JTokenType.Null)
							continue;
						queryResult.Projection[property.Name] = property.Value;
					}
				}
			}

			return new JsonDocument
			{
				Key = queryResult.Key,
				Projection = queryResult.Projection,
			};
		}


		private JsonDocument GetDocumentWithCaching(string key)
		{
			if (key == null)
				return null;
			JsonDocument doc;
			if (cache.TryGetValue(key, out doc))
				return doc;
			doc = actions.Documents.DocumentByKey(key, null);
            EnsureIdInMetadata(doc);
			cache[key] = doc;
			return doc;
		}

	    public static void EnsureIdInMetadata(IJsonDocumentMetadata doc)
	    {
			if (doc == null)
				return;

	    	var metadata = doc.Metadata;
	    	if (metadata == null)
                return ;

            if (metadata.Property("@id") != null)
                metadata.Remove("@id");
            metadata.Add("@id", new JValue(doc.Key));
	    }

	    public bool ShouldIncludeResultInQuery(IndexQueryResult arg, IndexDefinition indexDefinition, FieldsToFetch fieldsToFetch)
		{
			var doc = RetrieveDocumentInternal(arg, loadedIdsForFilter, fieldsToFetch, indexDefinition);
			if (doc == null)
				return false;
			doc = ProcessReadVetoes(doc, null, ReadOperation.Query);
			return doc != null;
		}

		public T ProcessReadVetoes<T>(T document, TransactionInformation transactionInformation, ReadOperation operation)
			where T : class, IJsonDocumentMetadata, new()
		{
			if (document == null)
				return document;
			foreach (var readTrigger in triggers)
			{
				var readVetoResult = readTrigger.Value.AllowRead(document.Key, document.Metadata, operation, transactionInformation);
				switch (readVetoResult.Veto)
				{
					case ReadVetoResult.ReadAllow.Allow:
						break;
					case ReadVetoResult.ReadAllow.Deny:
						return new T
						{
							Metadata = new JObject(
								new JProperty("Raven-Read-Veto", new JObject(new JProperty("Reason", readVetoResult.Reason),
																			 new JProperty("Trigger", readTrigger.ToString())
																	))
								)
						};
					case ReadVetoResult.ReadAllow.Ignore:
						return null;
					default:
						throw new ArgumentOutOfRangeException(readVetoResult.Veto.ToString());
				}
			}

			return document;
		}

		public dynamic Load(string id)
		{
			var document = GetDocumentWithCaching(id);
			if(document == null)
				return new DynamicNullObject();
			return new DynamicJsonObject(document.ToJson());
		}

		public dynamic Load(object maybeId)
		{
			if (maybeId == null || maybeId is DynamicNullObject)
				return new DynamicNullObject();
			var id = maybeId as string;
			if (id != null)
				return Load(id);
			var jId = maybeId as JValue;
			if (jId != null)
				return Load(jId.Value.ToString());

			var items = new List<dynamic>();
			foreach (var itemId in (IEnumerable)maybeId)
			{
				items.Add(Load(itemId));
			}
			return new DynamicJsonObject.DynamicList(items.ToArray());
		}
	}
}
