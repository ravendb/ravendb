//-----------------------------------------------------------------------
// <copyright file="DocumentRetriever.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
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
    internal class DocumentRetriever : ITranslatorDatabaseAccessor
    {
        private static readonly ILog log = LogManager.GetCurrentClassLogger();

        private readonly IDictionary<string, JsonDocument> cache = new Dictionary<string, JsonDocument>(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> loadedIdsForRetrieval = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> loadedIdsForFilter = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> loadedIdsForProjectionRetrievals = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> loadedIdsForProjectionFilter = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly InMemoryRavenConfiguration configuration;
        private readonly IStorageActionsAccessor actions;
        private readonly OrderedPartCollection<AbstractReadTrigger> triggers;
        private readonly Dictionary<string, RavenJToken> transformerParameters;
        private readonly HashSet<string> itemsToInclude;
        private bool disableCache;

        public Etag Etag = Etag.Empty;

        public DocumentRetriever(InMemoryRavenConfiguration configuration, IStorageActionsAccessor actions, OrderedPartCollection<AbstractReadTrigger> triggers,
            Dictionary<string, RavenJToken> transformerParameters = null,
            HashSet<string> itemsToInclude = null)
        {
            this.configuration = configuration;
            this.actions = actions;
            this.triggers = triggers;
            this.transformerParameters = transformerParameters ?? new Dictionary<string, RavenJToken>();
            this.itemsToInclude = itemsToInclude ?? new HashSet<string>();
        }

        public JsonDocument RetrieveDocumentForQuery(IndexQueryResult queryResult, IndexDefinition indexDefinition, FieldsToFetch fieldsToFetch, bool skipDuplicateCheck)
        {
            return ExecuteReadTriggers(ProcessReadVetoes(
                RetrieveDocumentInternal(queryResult, loadedIdsForRetrieval, loadedIdsForProjectionRetrievals, fieldsToFetch, indexDefinition, skipDuplicateCheck),
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
                        ? (RavenJObject)resultingDocument.DataAsJson.CreateSnapshot()
                        : resultingDocument.DataAsJson,
                Metadata =
                    resultingDocument.Metadata.IsSnapshot
                        ? (RavenJObject)resultingDocument.Metadata.CreateSnapshot()
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
            HashSet<string> loadedProjections,
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
                var document = GetDocumentWithCaching(queryResult);
                if (document == null)
                    return null;

                document.Metadata = GetMetadata(document);

                if (skipDuplicateCheck == false)
                    document.Metadata[Constants.TemporaryScoreValue] = queryScore;

                return document;
            }

            JsonDocument doc = null;
            var hasTransformerInQuery = string.IsNullOrWhiteSpace(fieldsToFetch.Query.ResultsTransformer) == false;

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
                        //the flag AllowMultipleIndexEntriesForSameDocumentToResultTransformer only
                        //has meaning only if we have a transformer in the query processing pipeline
                        if (hasTransformerInQuery)
                        {
                            // duplicate document, filter it out
                            // the flag AllowMultipleIndexEntriesForSameDocumentToResultTransformer explicitly allows duplicates 							
                            if (loadedIds.Add(queryResult.Key) == false &&
                                fieldsToFetch.Query.AllowMultipleIndexEntriesForSameDocumentToResultTransformer == false)
                                return null;
                        }
                        else
                        {
                            // duplicate document, filter it out
                            if (loadedIds.Add(queryResult.Key) == false)
                                return null;
                        }
                    }
                    //here as well, the filtering makes sense only if we have a transformer in the query
                    else if (fieldsToFetch.Query.AllowMultipleIndexEntriesForSameDocumentToResultTransformer == false &&
                             hasTransformerInQuery) //we have a query with transformer
                    {
                        if (loadedProjections.Add(queryResult.Key) == false)
                            return null;
                    }
                }

                // We have to load the document if user explicitly asked for the id, since 
                // we normalize the casing for the document id on the index, and we need to return
                // the id to the user with the same casing they gave us.
                var fetchingId = fieldsToFetch.HasField(Constants.DocumentIdFieldName);
                var fieldsToFetchFromDocument = fieldsToFetch.Fields.Where(fieldToFetch => queryResult.Projection[fieldToFetch] == null).ToArray();
                if (fieldsToFetchFromDocument.Length > 0 || fetchingId)
                {
                    switch (configuration.ImplicitFetchFieldsFromDocumentMode)
                    {
                        case ImplicitFetchFieldsMode.Enabled:
                            doc = GetDocumentWithCaching(queryResult);
                            if (doc != null)
                            {
                                if (fetchingId)
                                {
                                    queryResult.Projection[Constants.DocumentIdFieldName] = doc.Key;
                                }

                                var result = doc.DataAsJson.SelectTokenWithRavenSyntax(fieldsToFetchFromDocument.ToArray());
                                foreach (var property in result)
                                {
                                    if (property.Value == null) continue;

                                    queryResult.Projection[property.Key] = property.Value;
                                }
                            }
                            break;
                        case ImplicitFetchFieldsMode.DoNothing:
                            break;
                        case ImplicitFetchFieldsMode.Exception:
                            string message = string.Format("Implicit fetching of fields from the document is disabled." + Environment.NewLine +
                                                  "Check your index ({0}) to make sure that all fields you want to project are stored in the index." + Environment.NewLine +
                                                  "You can control this behavior using the Raven/ImplicitFetchFieldsFromDocumentMode setting." + Environment.NewLine +
                                                  "Fields to fetch from document are: {1}" + Environment.NewLine +
                                                  "Fetching id: {2}", indexDefinition.Name, string.Join(", ", fieldsToFetchFromDocument), fetchingId);
                            throw new ImplicitFetchFieldsFromDocumentNotAllowedException(message);
                        default:
                            throw new ArgumentOutOfRangeException(configuration.ImplicitFetchFieldsFromDocumentMode.ToString());
                    }
                }
            }
            else if (fieldsToFetch.FetchAllStoredFields && string.IsNullOrEmpty(queryResult.Key) == false
                && (fieldsToFetch.Query == null || fieldsToFetch.Query.AllowMultipleIndexEntriesForSameDocumentToResultTransformer == false))
            {
                // duplicate document, filter it out
                if (loadedIds.Add(queryResult.Key) == false)
                    return null;

                doc = GetDocumentWithCaching(queryResult);
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
                return (RavenJObject)doc.Metadata.CreateSnapshot();

            return doc.Metadata;
        }

        private JsonDocument GetDocumentWithCaching(IndexQueryResult iqr)
        {
            if (iqr.DocumentLoaded)
                return iqr.Document;

            iqr.DocumentLoaded = true;
            iqr.Document = GetDocumentWithCaching(iqr.Key);
            if (iqr.Document != null)
                iqr.Key = iqr.Document.Key;// to get the actual document id in the right case sensitive manner
            return iqr.Document;
        }

        private JsonDocument GetDocumentWithCaching(string key)
        {
            if (key == null)
                return null;

            // first we check the dtc state, then the cache and the storage, to avoid race conditions
            var nonAuthoritativeInformationBehavior = actions.InFlightStateSnapshot.GetNonAuthoritativeInformationBehavior<JsonDocument>(null, key);

            JsonDocument doc;

            if (disableCache || cache.TryGetValue(key, out doc) == false)
            {
                doc = actions.Documents.DocumentByKey(key);
            }

            if (nonAuthoritativeInformationBehavior != null)
                doc = nonAuthoritativeInformationBehavior(doc);

            JsonDocument.EnsureIdInMetadata(doc);

            if (doc != null && doc.Metadata != null)
                doc.Metadata.EnsureCannotBeChangeAndEnableSnapshotting();

            if (disableCache == false && (doc == null || doc.NonAuthoritativeInformation != true))
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

        public bool ShouldIncludeResultInQuery(IndexQueryResult arg, IndexDefinition indexDefinition, FieldsToFetch fieldsToFetch, bool skipDuplicateCheck)
        {
            JsonDocument doc;
            if (arg.DocumentLoaded)
            {
                doc = arg.Document;
            }
            else
            {
                doc = RetrieveDocumentInternal(arg, loadedIdsForFilter, loadedIdsForProjectionFilter, fieldsToFetch, indexDefinition, skipDuplicateCheck);
                arg.Document = doc;
                arg.DocumentLoaded = true;
            }

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

            var items = new List<dynamic>();
            foreach (var itemId in (IEnumerable)maybeId)
            {
                var include = Include(itemId);// this is where the real work happens
                items.Add(include);
            }
            return new DynamicList(items);

        }
        public dynamic Include(string id)
        {
            ItemsToInclude.Add(id);
            return Load(id);
        }

        public dynamic Include(IEnumerable<string> ids)
        {
            var items = new List<dynamic>();
            foreach (var itemId in ids)
            {
                var include = Include(itemId);// this is where the real work happens
                items.Add(include);
            }
            return new DynamicList(items);
        }

        public dynamic Load(string id)
        {
            var document = GetDocumentWithCaching(id);
            document = ProcessReadVetoes(document, null, ReadOperation.Load);
            if (document == null)
            {
                Etag = Etag.HashWith(Etag.Empty);
                return new DynamicNullObject();
            }
            Etag = Etag.HashWith(document.Etag);
            if (document.Metadata.ContainsKey("Raven-Read-Veto"))
            {
                return new DynamicNullObject();
            }

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

        public Dictionary<string, RavenJToken> TransformerParameters { get { return this.transformerParameters; } }
        public HashSet<string> ItemsToInclude
        {
            get { return itemsToInclude; }
        }
    }
}
