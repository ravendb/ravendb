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
		private readonly IEnumerable<AbstractReadTrigger> triggers;

		private static readonly ThreadLocal<bool> disableReadTriggers = new ThreadLocal<bool>(() => false);

		public static IDisposable DisableReadTriggers()
		{
			var old = disableReadTriggers.Value;
			disableReadTriggers.Value = true;
			return new DisposableAction(() => disableReadTriggers.Value = old);
		}

		public DocumentRetriever(IStorageActionsAccessor actions, IEnumerable<AbstractReadTrigger> triggers)
		{
			this.actions = actions;
			this.triggers = triggers;
		}

		public JsonDocument RetrieveDocumentForQuery(IndexQueryResult queryResult, IndexDefinition indexDefinition, string[] fieldsToFetch, AggregationOperation aggregationOperation)
		{
			var doc = RetrieveDocumentInternal(queryResult, loadedIdsForRetrieval, fieldsToFetch, indexDefinition, aggregationOperation);
			return ExecuteReadTriggers(doc, null, ReadOperation.Query);
		}

		private JsonDocument RetrieveDocumentInternal(
			IndexQueryResult queryResult,
			HashSet<string> loadedIds,
			IEnumerable<string> fieldsToFetch, 
			IndexDefinition indexDefinition,
			AggregationOperation aggregationOperation)
		{
			if (queryResult.Projection == null)
			{
				// duplicate document, filter it out
				if (loadedIds.Add(queryResult.Key) == false)
					return null;
				return GetDocumentWithCaching(queryResult.Key);
			}

			if (fieldsToFetch != null)
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
				if (aggregationOperation != AggregationOperation.None)
				{
					var aggOpr = aggregationOperation & ~AggregationOperation.Dynamic;
					fieldsToFetch = fieldsToFetch.Concat(new[] {aggOpr.ToString()});
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

	    public DocumentRetriever EnsureIdInMetadata(JsonDocument doc)
	    {
            if (doc == null)
                return this;

            if (doc.Metadata == null)
                return this;

            if (doc.Metadata.Property("@id") != null)
                doc.Metadata.Remove("@id");
            doc.Metadata.Add("@id", new JValue(doc.Key));
	        return this;
	    }

	    public bool ShouldIncludeResultInQuery(IndexQueryResult arg, IndexDefinition indexDefinition, string[] fieldsToFetch, AggregationOperation aggregationOperation)
		{
			var doc = RetrieveDocumentInternal(arg, loadedIdsForFilter, fieldsToFetch, indexDefinition, aggregationOperation);
			if (doc == null)
				return false;
			doc = ProcessReadVetoes(doc, null, ReadOperation.Query);
			return doc != null;
		}

		public JsonDocument ExecuteReadTriggers(JsonDocument document, TransactionInformation transactionInformation, ReadOperation operation)
		{
			if(disableReadTriggers.Value)
				return document;

			return ExecuteReadTriggersOnRead(ProcessReadVetoes(document, transactionInformation, operation),
											 transactionInformation, operation);
		}

		private JsonDocument ExecuteReadTriggersOnRead(JsonDocument resultingDocument, TransactionInformation transactionInformation, ReadOperation operation)
		{
			if (resultingDocument == null)
				return null;

			foreach (var readTrigger in triggers)
			{
				readTrigger.OnRead(resultingDocument.Key, resultingDocument.DataAsJson, resultingDocument.Metadata, operation, transactionInformation);
			}
			return resultingDocument;
		}

		public JsonDocument ProcessReadVetoes(JsonDocument document, TransactionInformation transactionInformation, ReadOperation operation)
		{
			if (document == null)
				return document;
			foreach (var readTrigger in triggers)
			{
				var readVetoResult = readTrigger.AllowRead(document.Key, document.DataAsJson ?? document.Projection, document.Metadata, operation, transactionInformation);
				switch (readVetoResult.Veto)
				{
					case ReadVetoResult.ReadAllow.Allow:
						break;
					case ReadVetoResult.ReadAllow.Deny:
						return new JsonDocument
						{
							DataAsJson = new JObject(),
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
			return new DynamicJsonObject(document.DataAsJson);
		}

		public dynamic Load(object maybeId)
		{
			if (maybeId == null || maybeId is DynamicNullObject)
				return new DynamicNullObject();
			var id = maybeId as string;
			if (id != null)
				return Load(id);

			var items = new List<dynamic>();
			foreach (var itemId in (IEnumerable)maybeId)
			{
				items.Add(Load(itemId));
			}
			return new DynamicJsonObject.DynamicList(items.ToArray());
		}
	}
}
