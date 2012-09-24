//-----------------------------------------------------------------------
// <copyright file="DocumentRetriever.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Abstractions.Logging;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Abstractions.MEF;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Abstractions.Json;
using System.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Impl
{
	public class DocumentRetriever : ITranslatorDatabaseAccessor
	{
		private static ILog log = LogManager.GetCurrentClassLogger();

		private readonly IDictionary<string, JsonDocument> cache = new Dictionary<string, JsonDocument>(StringComparer.InvariantCultureIgnoreCase);
		private readonly HashSet<string> loadedIdsForRetrieval = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly HashSet<string> loadedIdsForFilter = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly IStorageActionsAccessor actions;
		private readonly OrderedPartCollection<AbstractReadTrigger> triggers;
		private readonly HashSet<string> itemsToInclude;

		public DocumentRetriever(IStorageActionsAccessor actions, OrderedPartCollection<AbstractReadTrigger> triggers,
			HashSet<string> itemsToInclude = null)
		{
			this.actions = actions;
			this.triggers = triggers;
			this.itemsToInclude = itemsToInclude ?? new HashSet<string>();
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
			var queryScore = queryResult.Score;

			if (float.IsNaN(queryScore))
				queryScore = 0f;

			if (queryResult.Projection == null)
			{
				// duplicate document, filter it out
				if (loadedIds.Add(queryResult.Key) == false)
					return null;
				var document = GetDocumentWithCaching(queryResult.Key);
				if (document != null)
					document.Metadata[Constants.TemporaryScoreValue] = queryScore;
				return document;
			}

			if (fieldsToFetch.IsProjection)
			{
				if (indexDefinition.IsMapReduce == false)
				{
					bool hasStoredFields = false;
					FieldStorage value;
					if(indexDefinition.Stores.TryGetValue(Constants.AllFields, out value))
					{
						hasStoredFields = value != FieldStorage.No;
					}
					foreach (var fieldToFetch in fieldsToFetch.Fields)
					{
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

				// We have to load the document if user explicitly asked for the id, since 
				// we normalize the casing for the document id on the index, and we need to return
				// the id to the user with the same casing they gave us.
				var fetchingId = fieldsToFetch.Fields.Any(fieldToFetch => fieldToFetch == Constants.DocumentIdFieldName);
				var fieldsToFetchFromDocument = fieldsToFetch.Fields
					.Where(fieldToFetch => queryResult.Projection[fieldToFetch] == null)
					.ToArray();
				if (fieldsToFetchFromDocument.Length > 0 || fetchingId)
				{
					var doc = GetDocumentWithCaching(queryResult.Key);
					if (doc != null)
					{
						if(fetchingId) 
						{
							queryResult.Projection[Constants.DocumentIdFieldName] = doc.Key;
						}

						var result = doc.DataAsJson.SelectTokenWithRavenSyntax(fieldsToFetchFromDocument.ToArray());
						foreach (var property in result)
						{
							if (property.Value == null || property.Value.Type == JTokenType.Null)
								continue;
							queryResult.Projection[property.Key] = property.Value;
						}
					}
				}
			}

			return new JsonDocument
			{
				Key = queryResult.Key,
				DataAsJson = queryResult.Projection,
				Metadata = new RavenJObject{{Constants.TemporaryScoreValue, queryScore}}
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
			if (doc == null || doc.Metadata == null)
				return;

			doc.Metadata["@id"] = new RavenJValue(doc.Key);
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
									Etag = Guid.Empty,
									LastModified = DateTime.MinValue,
						       		NonAuthoritativeInformation = false,
									Metadata = new RavenJObject
						       		           	{
						       		           		{
						       		           			"Raven-Read-Veto", new RavenJObject
						       		           			                   	{
						       		           			                   		{"Reason", readVetoResult.Reason},
						       		           			                   		{"Trigger", readTrigger.ToString()}
						       		           			                   	}
						       		           			}
						       		           	}
						       	};
					case ReadVetoResult.ReadAllow.Ignore:
						log.Debug("Trigger {0} asked us to ignore {1}", readTrigger.Value, document.Key);
						return null;
					default:
						throw new ArgumentOutOfRangeException(readVetoResult.Veto.ToString());
				}
			}

			return document;
		}

		public dynamic Include(object maybeId)
		{
			if (maybeId == null || maybeId is DynamicNullObject)
				return new DynamicNullObject();
			var id = maybeId as string;
			if (id != null)
				return Include(id);
			var jId = maybeId as RavenJValue;
			if (jId != null)
				return Include(jId.Value.ToString());

			foreach (var itemId in (IEnumerable)maybeId)
			{
				Include(itemId);
			}
			return new DynamicNullObject();

		}
		public dynamic Include(string id)
		{
			itemsToInclude.Add(id);
			return new DynamicNullObject();
		}
		
		public dynamic Include(IEnumerable<string> ids)
		{
			foreach (var id in ids)
			{
				itemsToInclude.Add(id);
			}
			return new DynamicNullObject();
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
			var jId = maybeId as RavenJValue;
			if (jId != null)
				return Load(jId.Value.ToString());

			var items = new List<dynamic>();
			foreach (var itemId in (IEnumerable)maybeId)
			{
				items.Add(Load(itemId));
			}
			return new DynamicList(items.Select(x => (object) x).ToArray());
		}
	}
}
