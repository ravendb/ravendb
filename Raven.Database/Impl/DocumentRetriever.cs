//-----------------------------------------------------------------------
// <copyright file="DocumentRetriever.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Abstractions.Logging;
using Raven.Database.Impl.DTC;
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
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		private readonly IDictionary<string, JsonDocument> cache = new Dictionary<string, JsonDocument>(StringComparer.OrdinalIgnoreCase);
		private readonly HashSet<string> loadedIdsForRetrieval = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		private readonly HashSet<string> loadedIdsForFilter = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
		private readonly IStorageActionsAccessor actions;
		private readonly OrderedPartCollection<AbstractReadTrigger> triggers;
		private readonly InFlightTransactionalState inFlightTransactionalState;
		private readonly Dictionary<string, RavenJToken> queryInputs;
	    private readonly HashSet<string> itemsToInclude;
		private bool disableCache;

	    public Etag Etag = Etag.Empty;

		public DocumentRetriever(IStorageActionsAccessor actions, OrderedPartCollection<AbstractReadTrigger> triggers, 
			InFlightTransactionalState inFlightTransactionalState,
            Dictionary<string, RavenJToken> queryInputs = null,
            HashSet<string> itemsToInclude = null)
		{
			this.actions = actions;
			this.triggers = triggers;
			this.inFlightTransactionalState = inFlightTransactionalState;
			this.queryInputs = queryInputs ?? new Dictionary<string, RavenJToken>();
		    this.itemsToInclude = itemsToInclude ?? new HashSet<string>();
		}

		public JsonDocument RetrieveDocumentForQuery(IndexQueryResult queryResult, IndexDefinition indexDefinition, FieldsToFetch fieldsToFetch, bool skipDuplicateCheck)
		{
			return ExecuteReadTriggers(ProcessReadVetoes(
				RetrieveDocumentInternal(queryResult, loadedIdsForRetrieval, fieldsToFetch, indexDefinition, skipDuplicateCheck),
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

			var doc = new JsonDocument
			{
				Key = resultingDocument.Key,
				Etag = resultingDocument.Etag,
				LastModified = resultingDocument.LastModified,
				SerializedSizeOnDisk = resultingDocument.SerializedSizeOnDisk,
				SkipDeleteFromIndex = resultingDocument.SkipDeleteFromIndex,
				NonAuthoritativeInformation = resultingDocument.NonAuthoritativeInformation,
				TempIndexScore = resultingDocument.TempIndexScore,
				DataAsJson =
					resultingDocument.DataAsJson.IsSnapshot
						? (RavenJObject) resultingDocument.DataAsJson.CreateSnapshot()
						: resultingDocument.DataAsJson,
				Metadata =
					resultingDocument.Metadata.IsSnapshot
						? (RavenJObject) resultingDocument.Metadata.CreateSnapshot()
						: resultingDocument.Metadata,
			};

			triggers.Apply(
				trigger =>
				trigger.OnRead(doc.Key, doc.DataAsJson, doc.Metadata, operation,
							   transactionInformation));

			return doc;
		}

		private JsonDocument RetrieveDocumentInternal(
			IndexQueryResult queryResult,
			HashSet<string> loadedIds,
			FieldsToFetch fieldsToFetch,
			IndexDefinition indexDefinition,
			bool skipDuplicateCheck)
		{
			var queryScore = queryResult.Score;

			if (float.IsNaN(queryScore))
				queryScore = 0f;

			if (queryResult.Projection == null)
			{
				// duplicate document, filter it out
				if (skipDuplicateCheck == false && loadedIds.Add(queryResult.Key) == false)
					return null;
				var document = GetDocumentWithCaching(queryResult.Key);
				if (document != null)
				{
					if(skipDuplicateCheck == false)
						document.Metadata[Constants.TemporaryScoreValue] = queryScore;
				}
				return document;
			}

			JsonDocument doc = null;

		    if (fieldsToFetch.IsProjection)
		    {
		        if (indexDefinition.IsMapReduce == false)
		        {
		            bool hasStoredFields = false;
		            FieldStorage value;
		            if (indexDefinition.Stores.TryGetValue(Constants.AllFields, out value))
		            {
		                hasStoredFields = value != FieldStorage.No;
		            }
		            foreach (var fieldToFetch in fieldsToFetch.Fields)
		            {
		                if (indexDefinition.Stores.TryGetValue(fieldToFetch, out value) == false && value != FieldStorage.No) continue;
		                hasStoredFields = true;
		            }
		            if (hasStoredFields == false)
		            {
		                // duplicate document, filter it out
		                if (loadedIds.Add(queryResult.Key) == false) return null;
		            }
		        }

		        // We have to load the document if user explicitly asked for the id, since 
		        // we normalize the casing for the document id on the index, and we need to return
		        // the id to the user with the same casing they gave us.
		        var fetchingId = fieldsToFetch.HasField(Constants.DocumentIdFieldName);
		        var fieldsToFetchFromDocument = fieldsToFetch.Fields.Where(fieldToFetch => queryResult.Projection[fieldToFetch] == null).ToArray();
		        if (fieldsToFetchFromDocument.Length > 0 || fetchingId)
		        {
		            doc = GetDocumentWithCaching(queryResult.Key);
		            if (doc != null)
		            {
		                if (fetchingId)
		                {
		                    queryResult.Projection[Constants.DocumentIdFieldName] = doc.Key;
		                }

		                var result = doc.DataAsJson.SelectTokenWithRavenSyntax(fieldsToFetchFromDocument.ToArray());
		                foreach (var property in result)
		                {
		                    if (property.Value == null || property.Value.Type == JTokenType.Null) continue;
		                    queryResult.Projection[property.Key] = property.Value;
		                }
		            }
		        }
		    }
			else if (fieldsToFetch.FetchAllStoredFields && string.IsNullOrEmpty(queryResult.Key) == false)
		    {
                // duplicate document, filter it out
                if (loadedIds.Add(queryResult.Key) == false)
                    return null;

                doc = GetDocumentWithCaching(queryResult.Key);
		    }

			var metadata = GetMetadata(doc);
			metadata.Remove("@id");
			metadata[Constants.TemporaryScoreValue] = queryScore;
			return new JsonDocument
			{
				Key = queryResult.Key,
				DataAsJson = queryResult.Projection,
				Metadata = metadata
			};
		}

		private static RavenJObject GetMetadata(JsonDocument doc)
		{
			if (doc == null)
				return new RavenJObject();

			if (doc.Metadata.IsSnapshot)
				return (RavenJObject) doc.Metadata.CreateSnapshot();

			return doc.Metadata;
		}


		private JsonDocument GetDocumentWithCaching(string key)
		{
			if (key == null)
				return null;
			JsonDocument doc;
			if (disableCache == false && cache.TryGetValue(key, out doc))
				return doc;
			doc = actions.Documents.DocumentByKey(key, null);
			EnsureIdInMetadata(doc);
			var nonAuthoritativeInformationBehavior = inFlightTransactionalState.GetNonAuthoritativeInformationBehavior<JsonDocument>(null, key);
			if (nonAuthoritativeInformationBehavior != null)
				doc = nonAuthoritativeInformationBehavior(doc);
			if(disableCache == false)
				cache[key] = doc;
			if (cache.Count > 2048)
			{
				// we are probably doing a stream here, no point in trying to cache things, we might be
				// going through the entire db here!
				disableCache = true;
				cache.Clear();
			}
			return doc;
		}

		public static void EnsureIdInMetadata(IJsonDocumentMetadata doc)
		{
			if (doc == null || doc.Metadata == null)
				return;

			if (doc.Metadata.IsSnapshot)
			{
				doc.Metadata = (RavenJObject)doc.Metadata.CreateSnapshot();
			}

			doc.Metadata["@id"] = doc.Key;
		}

		public bool ShouldIncludeResultInQuery(IndexQueryResult arg, IndexDefinition indexDefinition, FieldsToFetch fieldsToFetch, bool skipDuplicateCheck)
		{
			var doc = RetrieveDocumentInternal(arg, loadedIdsForFilter, fieldsToFetch, indexDefinition, skipDuplicateCheck);
			if (doc == null)
				return false;
			doc = ProcessReadVetoes(doc, null, ReadOperation.Query);
			return doc != null;
		}

		public T ProcessReadVetoes<T>(T document, TransactionInformation transactionInformation, ReadOperation operation)
			where T : class, IJsonDocumentMetadata, new()
		{
			if (document == null)
				return null;
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
									Etag = Etag.Empty,
									LastModified = DateTime.MinValue,
									NonAuthoritativeInformation = false,
									Key = document.Key,
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
			if (document == null)
			{
			    Etag = Etag.HashWith(Etag.Empty);
			    return new DynamicNullObject();
			}
		    Etag = Etag.HashWith(document.Etag);
			return new DynamicJsonObject(document.ToJson());
		}

		public dynamic Load(object maybeId)
		{
			if (maybeId == null || maybeId is DynamicNullObject)
			{
			    Etag = Etag.HashWith(Etag.Empty);
			    return new DynamicNullObject();
			}
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
			return new DynamicList(items.Select(x => (object)x).ToArray());
		}

        public Dictionary<string, RavenJToken> QueryInputs { get { return this.queryInputs; } } 
	}
}
