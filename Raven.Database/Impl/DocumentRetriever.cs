using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Http;

namespace Raven.Database.Impl
{
    public class DocumentRetriever : ITranslatorDatabaseAccessor
    {
        private readonly IDictionary<string, JsonDocument> cache = new Dictionary<string, JsonDocument>(StringComparer.InvariantCultureIgnoreCase);
        private readonly HashSet<string> loadedIdsForRetrieval = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
        private readonly HashSet<string> loadedIdsForFilter = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
        private readonly IStorageActionsAccessor actions;
        private readonly IEnumerable<AbstractReadTrigger> triggers;

        public DocumentRetriever(IStorageActionsAccessor actions, IEnumerable<AbstractReadTrigger> triggers)
        {
            this.actions = actions;
            this.triggers = triggers;
        }

        public JsonDocument RetrieveDocumentForQuery(IndexQueryResult queryResult, IndexDefinition indexDefinition, string[] fieldsToFetch)
        {
            var doc = RetrieveDocumentInternal(queryResult, loadedIdsForRetrieval, fieldsToFetch, indexDefinition);
            return ExecuteReadTriggers(doc, null, ReadOperation.Query);
        }

        private JsonDocument RetrieveDocumentInternal(IndexQueryResult queryResult, HashSet<string> loadedIds, string[] fieldsToFetch, IndexDefinition indexDefinition)
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
                foreach (var fieldToFetch in fieldsToFetch)
                {
                    if (queryResult.Projection.Property(fieldToFetch) != null)
                        continue;

                    var doc = GetDocumentWithCaching(queryResult.Key);
                    var token = doc.DataAsJson.SelectToken(fieldToFetch);
                    queryResult.Projection[fieldToFetch] = token;
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
            cache[key] = doc;
            return doc;
        }

        public bool ShouldIncludeResultInQuery(IndexQueryResult arg, IndexDefinition indexDefinition, string[] fieldsToFetch)
        {
            var doc = RetrieveDocumentInternal(arg, loadedIdsForFilter, fieldsToFetch, indexDefinition);
            if (doc == null)
                return false;
            doc = ProcessReadVetoes(doc, null, ReadOperation.Query);
            return doc != null;
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
                return null;
            return new DynamicJsonObject(document.DataAsJson);
        }
    }
}
