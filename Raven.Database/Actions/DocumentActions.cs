// -----------------------------------------------------------------------
//  <copyright file="DocumentActions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Linq;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Raven.Json.Linq;

namespace Raven.Database.Actions
{
    public class DocumentActions : ActionsBase
    {
        public DocumentActions(DocumentDatabase database, IUuidGenerator uuidGenerator, ILog log)
            : base(database, uuidGenerator, log)
        {
        }

        public long GetNextIdentityValueWithoutOverwritingOnExistingDocuments(string key,
            IStorageActionsAccessor actions)
        {
            int tries;
            return GetNextIdentityValueWithoutOverwritingOnExistingDocuments(key, actions, out tries);
        }

        public long GetNextIdentityValueWithoutOverwritingOnExistingDocuments(string key,
            IStorageActionsAccessor actions,
            out int tries)
        {
            var nextIdentityValue = actions.General.GetNextIdentityValue(key);

            if (actions.Documents.DocumentMetadataByKey(key + nextIdentityValue) == null)
            {
                tries = 1;
                return nextIdentityValue;
            }
            tries = 1;
            // there is already a document with this id, this means that we probably need to search
            // for an opening in potentially large data set. 
            var lastKnownBusy = nextIdentityValue;
            var maybeFree = nextIdentityValue*2;
            var lastKnownFree = long.MaxValue;
            while (true)
            {
                tries++;
                if (actions.Documents.DocumentMetadataByKey(key + maybeFree) == null)
                {
                    if (lastKnownBusy + 1 == maybeFree)
                    {
                        actions.General.SetIdentityValue(key, maybeFree);
                        return maybeFree;
                    }
                    lastKnownFree = maybeFree;
                    maybeFree = Math.Max(maybeFree - (maybeFree - lastKnownBusy)/2, lastKnownBusy + 1);
                }
                else
                {
                    lastKnownBusy = maybeFree;
                    maybeFree = Math.Min(lastKnownFree, maybeFree*2);
                }
            }
        }


        private void AssertPutOperationNotVetoed(string key, RavenJObject metadata, RavenJObject document, TransactionInformation transactionInformation)
        {
            var vetoResult = Database.PutTriggers
                .Select(trigger => new {Trigger = trigger, VetoResult = trigger.AllowPut(key, document, metadata, transactionInformation)})
                .FirstOrDefault(x => x.VetoResult.IsAllowed == false);
            if (vetoResult != null)
            {
                throw new OperationVetoedException("PUT vetoed on document " + key + " by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
            }
        }

        public RavenJArray GetDocumentsWithIdStartingWith(string idPrefix, string matches, string exclude, int start,
            int pageSize, CancellationToken token, ref int nextStart,
            string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null,
            string skipAfter = null)
        {
            using (DocumentCacher.SkipSetDocumentsInDocumentCache())
            {
                var list = new RavenJArray();
                GetDocumentsWithIdStartingWith(idPrefix, matches, exclude, start, pageSize, token, ref nextStart,
                    doc => { if (doc != null) list.Add(doc.ToJson()); }, transformer, transformerParameters, skipAfter);

                return list;
            }  
        }

        public void GetDocumentsWithIdStartingWith(string idPrefix, string matches, string exclude, int start, int pageSize,
            CancellationToken token, ref int nextStart, Action<JsonDocument> addDoc,
            string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null,
            string skipAfter = null)
        {
            if (idPrefix == null)
                throw new ArgumentNullException("idPrefix");
            idPrefix = idPrefix.Trim();

            var canPerformRapidPagination = nextStart > 0 && start == nextStart;
            var actualStart = canPerformRapidPagination ? start : 0;
            var addedDocs = 0;
            var docCountOnLastAdd = 0;
            var matchedDocs = 0;

            TransactionalStorage.Batch(
                actions =>
                {
                    var docsToSkip = canPerformRapidPagination ? 0 : start;
                    int docCount;

                    AbstractTransformer storedTransformer = null;
                    var hasTransformer = transformer != null;
                    if (hasTransformer)
                    {
                        storedTransformer = IndexDefinitionStorage.GetTransformer(transformer);
                        if (storedTransformer == null)
                            throw new InvalidOperationException("No transformer with the name: " + transformer);
                    }

                    do
                    {
                        Database.WorkContext.UpdateFoundWork();

                        docCount = 0;
                        var docs = actions.Documents.GetDocumentsWithIdStartingWith(idPrefix, actualStart, pageSize, string.IsNullOrEmpty(skipAfter) ? null : skipAfter);
                        var documentRetriever = new DocumentRetriever(Database.Configuration, actions, Database.ReadTriggers, transformerParameters, hasTransformer: hasTransformer);

                        foreach (var doc in docs)
                        {
                            token.ThrowIfCancellationRequested();
                            docCount++;
                            if (docCount - docCountOnLastAdd > 1000)
                            {
                                addDoc(null); // heartbeat
                            }

                            var keyTest = doc.Key.Substring(idPrefix.Length);

                            if (!WildcardMatcher.Matches(matches, keyTest) || WildcardMatcher.MatchesExclusion(exclude, keyTest))
                                continue;

                            JsonDocument.EnsureIdInMetadata(doc);
                            var nonAuthoritativeInformationBehavior = actions.InFlightStateSnapshot.GetNonAuthoritativeInformationBehavior<JsonDocument>(null, doc.Key);

                            var document = nonAuthoritativeInformationBehavior != null ? nonAuthoritativeInformationBehavior(doc) : doc;
                            document = documentRetriever.ExecuteReadTriggers(document, null, ReadOperation.Load);
                            if (document == null)
                                continue;

                            matchedDocs++;

                            if (matchedDocs <= docsToSkip)
                                continue;

                            token.ThrowIfCancellationRequested();

                            document = TransformDocumentIfNeeded(document, storedTransformer, documentRetriever);
                            addDoc(document);

                            addedDocs++;
                            docCountOnLastAdd = docCount;

                            if (addedDocs >= pageSize)
                                break;
                        }

                        actualStart += docCount;
                    } while (docCount > 0 && addedDocs < pageSize && actualStart > 0 && actualStart < int.MaxValue);
                });

            if (addedDocs != pageSize)
                nextStart = start; // will mark as last page
            else if (canPerformRapidPagination)
                nextStart = start + matchedDocs;
            else
                nextStart = actualStart;
        }

        private JsonDocument TransformDocumentIfNeeded(JsonDocument document, AbstractTransformer storedTransformer, DocumentRetriever documentRetriever)
        {
            if (storedTransformer == null)
                return document;

            using (new CurrentTransformationScope(Database, documentRetriever))
            {
                var transformed = storedTransformer
                    .TransformResultsDefinition(new[] {new DynamicJsonObject(document.ToJson())})
                    .Select<dynamic, dynamic>(x => JsonExtensions.ToJObject((object) x))
                    .ToArray();

                RavenJObject ravenJObject;
                switch (transformed.Length)
                {
                    case 0:
                        throw new InvalidOperationException("The transform results function failed on a document: " + document.Key);
                    case 1:
                        ravenJObject = transformed[0];
                        break;
                    default:
                        ravenJObject = new RavenJObject {{"$values", new RavenJArray(transformed)}};
                        break;
                }

                return new JsonDocument
                {
                    Etag = document.Etag.HashWith(storedTransformer.GetHashCodeBytes()).HashWith(documentRetriever.Etag),
                    NonAuthoritativeInformation = document.NonAuthoritativeInformation,
                    LastModified = document.LastModified,
                    DataAsJson = ravenJObject
                };
            }
        }

        private void RemoveMetadataReservedProperties(RavenJObject metadata, InvokeSource source = InvokeSource.Default)
        {
            RemoveReservedProperties(metadata);
            metadata.Remove("Raven-Last-Modified");
            if (source != InvokeSource.FromConflictAtReplication)
            {
                metadata.Remove("Last-Modified");
            }
        }

        private void RemoveReservedProperties(RavenJObject document)
        {
            document.Remove(string.Empty);
            var toRemove = document.Keys.Where(propertyName => propertyName.StartsWith("@") || HeadersToIgnoreServer.Contains(propertyName) || Database.Configuration.HeadersToIgnore.Contains(propertyName)).ToList();
            foreach (var propertyName in toRemove)
            {
                document.Remove(propertyName);
            }
        }

        private void AssertDeleteOperationNotVetoed(string key, TransactionInformation transactionInformation)
        {
            var vetoResult = Database.DeleteTriggers
                .Select(trigger => new {Trigger = trigger, VetoResult = trigger.AllowDelete(key, transactionInformation)})
                .FirstOrDefault(x => x.VetoResult.IsAllowed == false);
            if (vetoResult != null)
            {
                throw new OperationVetoedException("DELETE vetoed on document " + key + " by " + vetoResult.Trigger +
                                                   " because: " + vetoResult.VetoResult.Reason);
            }
        }

        public int BulkInsert(BulkInsertOptions options, IEnumerable<IEnumerable<JsonDocument>> docBatches, Guid operationId, CancellationToken token, CancellationTimeout timeout = null)
        {
            var documents = 0;

            Database.Notifications.RaiseNotifications(new BulkInsertChangeNotification
            {
                OperationId = operationId,
                Type = DocumentChangeTypes.BulkInsertStarted
            });
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, WorkContext.CancellationToken))
            {
                foreach (var docs in docBatches)
                {
                    cts.Token.ThrowIfCancellationRequested();

                    var docsToInsert = docs.ToArray();
                    var batch = 0;
                    var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    var collectionsAndEtags = new Dictionary<string, Etag>(StringComparer.OrdinalIgnoreCase);

                    timeout?.Pause();
                    using (Database.DocumentLock.Lock())
                    {
                        timeout?.Resume();
                        TransactionalStorage.Batch(accessor =>
                        {
                            var inserts = 0;

                            foreach (var doc in docsToInsert)
                            {
                                try
                                {
                                    if (string.IsNullOrWhiteSpace(doc.Key))
                                        throw new InvalidOperationException("Cannot try to bulk insert a document without a key");

                                    doc.Key = doc.Key.Trim();
                                    if (doc.Key[doc.Key.Length - 1] == '/')
                                    {
                                        doc.Key += GetNextIdentityValueWithoutOverwritingOnExistingDocuments(doc.Key, accessor);
                                    }

                                    RemoveReservedProperties(doc.DataAsJson);
                                    RemoveMetadataReservedProperties(doc.Metadata);

                                    if (options.CheckReferencesInIndexes)
                                        keys.Add(doc.Key);
                                    documents++;
                                    batch++;
                                    AssertPutOperationNotVetoed(doc.Key, doc.Metadata, doc.DataAsJson, null);

                                    if (options.OverwriteExisting && options.SkipOverwriteIfUnchanged)
                                    {
                                        var existingDoc = accessor.Documents.DocumentByKey(doc.Key);

                                        if (IsTheSameDocument(doc, existingDoc))
                                            continue;
                                    }

                                    foreach (var trigger in Database.PutTriggers)
                                    {
                                        trigger.Value.OnPut(doc.Key, doc.DataAsJson, doc.Metadata, null);
                                    }

                                    var result = accessor.Documents.InsertDocument(doc.Key, doc.DataAsJson, doc.Metadata, options.OverwriteExisting);
                                    if (result.Updated == false)
                                        inserts++;

                                    doc.Etag = result.Etag;

                                    doc.Metadata.EnsureSnapshot(
                                        "Metadata was written to the database, cannot modify the document after it was written (changes won't show up in the db). Did you forget to call CreateSnapshot() to get a clean copy?");
                                    doc.DataAsJson.EnsureSnapshot(
                                        "Document was written to the database, cannot modify the document after it was written (changes won't show up in the db). Did you forget to call CreateSnapshot() to get a clean copy?");

                                    var entityName = doc.Metadata.Value<string>(Constants.RavenEntityName);

                                    Etag highestEtagInCollection;
                                    if (string.IsNullOrEmpty(entityName) == false && (collectionsAndEtags.TryGetValue(entityName, out highestEtagInCollection) == false ||
                                                                                      result.Etag.CompareTo(highestEtagInCollection) > 0))
                                    {
                                        collectionsAndEtags[entityName] = result.Etag;
                                    }

                                    foreach (var trigger in Database.PutTriggers)
                                    {
                                        trigger.Value.AfterPut(doc.Key, doc.DataAsJson, doc.Metadata, result.Etag, null);
                                    }

                                    Database.WorkContext.UpdateFoundWork();
                                }
                                catch (Exception e)
                                {
                                    Database.Notifications.RaiseNotifications(new BulkInsertChangeNotification
                                    {
                                        OperationId = operationId,
                                        Message = e.Message,
                                        Etag = doc.Etag,
                                        Id = doc.Key,
                                        Type = DocumentChangeTypes.BulkInsertError
                                    });

                                    throw;
                                }
                            }

                            if (options.CheckReferencesInIndexes)
                                foreach (var key in keys)
                                    Database.Indexes.CheckReferenceBecauseOfDocumentUpdate(key, accessor);

                            accessor.Documents.IncrementDocumentCount(inserts);
                        });

                        foreach (var collectionEtagPair in collectionsAndEtags)
                        {
                            Database.LastCollectionEtags.Update(collectionEtagPair.Key, collectionEtagPair.Value);
                        }

                        WorkContext.ShouldNotifyAboutWork(() => "BulkInsert batch of " + batch + " docs");
                        WorkContext.NotifyAboutWork(); // forcing notification so we would start indexing right away
                        WorkContext.UpdateFoundWork();
                    }
                }
            }

            Database.Notifications.RaiseNotifications(new BulkInsertChangeNotification
            {
                OperationId = operationId,
                Type = DocumentChangeTypes.BulkInsertEnded
            });

            if (documents > 0)
                WorkContext.ShouldNotifyAboutWork(() => "BulkInsert of " + documents + " docs");

            return documents;
        }

        private bool IsTheSameDocument(JsonDocument doc, JsonDocument existingDoc)
        {
            if (existingDoc == null)
                return false;

            if (RavenJToken.DeepEquals(doc.DataAsJson, existingDoc.DataAsJson) == false)
                return false;

            var existingMetadata = (RavenJObject) existingDoc.Metadata.CloneToken();
            var newMetadata = (RavenJObject) doc.Metadata.CloneToken();
            // in order to compare metadata we need to remove metadata records created by triggers
            foreach (var trigger in Database.PutTriggers)
            {
                var metadataToIgnore = trigger.Value.GeneratedMetadataNames;

                if (metadataToIgnore == null)
                    continue;

                foreach (var toIgnore in metadataToIgnore)
                {
                    existingMetadata.Remove(toIgnore);
                    newMetadata.Remove(toIgnore);
                }
            }

            return RavenJToken.DeepEquals(newMetadata, existingMetadata);
        }

        public RavenJArray GetDocumentsAsJson(int start, int pageSize, Etag etag,
            CancellationToken token, long? maxSize = null, TimeSpan? timeout = null)
        {
            var list = new RavenJArray();
            GetDocuments(start, pageSize, etag, token, doc =>
            {
                if (doc != null) list.Add(doc.ToJson());
                return true;
            }, maxSize: maxSize, timeout: timeout);
            return list;
        }

        public Etag GetDocuments(int start, int pageSize, Etag etag, CancellationToken token, Func<JsonDocument, bool> addDocument, 
            string transformer = null, Dictionary<string, RavenJToken> transformerParameters = null, 
            long? maxSize = null, TimeSpan? timeout = null, HashSet<string> collections = null)
        {
            Etag lastDocumentReadEtag = null;

            using (DocumentCacher.SkipSetDocumentsInDocumentCache())
                TransactionalStorage.Batch(actions =>
                {
                    AbstractTransformer storedTransformer = null;
                    var hasTransformer = transformer != null;
                    if (hasTransformer)
                    {
                        storedTransformer = IndexDefinitionStorage.GetTransformer(transformer);
                        if (storedTransformer == null)
                            throw new InvalidOperationException("No transformer with the name: " + transformer);
                    }

                    var returnedDocs = false;
                    while (true)
                    {
                        var documents = etag == null
                            ? actions.Documents.GetDocumentsByReverseUpdateOrder(start, pageSize, entityNames: collections)
                            : actions.Documents.GetDocumentsAfter(etag, pageSize, token, maxSize: maxSize, timeout: timeout, entityNames: collections);

                        var documentRetriever = new DocumentRetriever(Database.Configuration, actions, Database.ReadTriggers, transformerParameters, hasTransformer: hasTransformer);
                        var docCount = 0;
                        var docCountOnLastAdd = 0;
                        foreach (var doc in documents)
                        {
                            docCount++;

                            token.ThrowIfCancellationRequested();

                            if (docCount - docCountOnLastAdd > 1000)
                            {
                                addDocument(null); // heartbeat
                            }

                            if (etag != null)
                                etag = doc.Etag;

                            JsonDocument.EnsureIdInMetadata(doc);

                            var nonAuthoritativeInformationBehavior = actions.InFlightStateSnapshot.GetNonAuthoritativeInformationBehavior<JsonDocument>(null, doc.Key);
                            var document = nonAuthoritativeInformationBehavior == null ? doc : nonAuthoritativeInformationBehavior(doc);

                            document = documentRetriever.ExecuteReadTriggers(document, null, ReadOperation.Load);
                            if (document == null)
                                continue;

                            returnedDocs = true;
                            Database.WorkContext.UpdateFoundWork();

                            document = TransformDocumentIfNeeded(document, storedTransformer, documentRetriever);

                            var canContinue = addDocument(document);
                            if (!canContinue)
                                break;

                            lastDocumentReadEtag = etag;

                            docCountOnLastAdd = docCount;
                        }

                        if (returnedDocs || docCount == 0)
                            break;

                        // No document was found that matches the requested criteria
                        // If we had a failure happen, we update the etag as we don't need to process those documents again (no matches there anyways).
                        if (lastDocumentReadEtag != null)
                            etag = lastDocumentReadEtag;

                        start += docCount;
                    }
                });

            return lastDocumentReadEtag;
        }

        public Etag GetDocumentsWithIdStartingWith(string idPrefix, int pageSize, Etag etag, 
            CancellationToken token, Func<JsonDocument, bool> addDocument, HashSet<string> collections)
        {
            Etag lastDocumentReadEtag = null;

            TransactionalStorage.Batch(actions =>
            {
                var returnedDocs = false;
                while (true)
                {
                    var documents = actions.Documents.GetDocumentsAfterWithIdStartingWith(etag, idPrefix, pageSize, token, 
                        timeout: TimeSpan.FromSeconds(2), lastProcessedDocument: x => lastDocumentReadEtag = x, entityNames: collections);

                    var documentRetriever = new DocumentRetriever(Database.Configuration, actions, Database.ReadTriggers);

                    var docCount = 0;
                    var docCountOnLastAdd = 0;
                    foreach (var doc in documents)
                    {
                        docCount++;
                        if (docCount - docCountOnLastAdd > 1000)
                        {
                            addDocument(null); // heartbeat
                        }

                        token.ThrowIfCancellationRequested();

                        etag = doc.Etag;

                        JsonDocument.EnsureIdInMetadata(doc);

                        var nonAuthoritativeInformationBehavior = actions.InFlightStateSnapshot.GetNonAuthoritativeInformationBehavior<JsonDocument>(null, doc.Key);
                        var document = nonAuthoritativeInformationBehavior == null ? doc : nonAuthoritativeInformationBehavior(doc);

                        document = documentRetriever.ExecuteReadTriggers(document, null, ReadOperation.Load);
                        if (document == null)
                            continue;

                        returnedDocs = true;
                        Database.WorkContext.UpdateFoundWork();

                        var canContinue = addDocument(document);

                        docCountOnLastAdd = docCount;

                        if (!canContinue)
                            break;
                    }

                    if (returnedDocs)
                        break;

                    // No document was found that matches the requested criteria
                    if (docCount == 0)
                    {
                        // If we had a failure happen, we update the etag as we don't need to process those documents again (no matches there anyways).
                        if (lastDocumentReadEtag != null)
                            etag = lastDocumentReadEtag;

                        break;
                    }
                }
            });

            return etag;
        }

        public JsonDocument Get(string key, TransactionInformation transactionInformation)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            key = key.Trim();

            JsonDocument document = null;
            if (transactionInformation == null ||
                Database.InFlightTransactionalState.TryGet(key, transactionInformation, out document) == false)
            {
                TransactionalStorage.Batch(actions =>
                {
                    var nonAuthoritativeInformationBehavior = actions.InFlightStateSnapshot.GetNonAuthoritativeInformationBehavior<JsonDocument>(transactionInformation, key);

                    document = actions.Documents.DocumentByKey(key);

                    if (nonAuthoritativeInformationBehavior != null)
                    {
                        document = nonAuthoritativeInformationBehavior(document);
                    }

                    if (document != null)
                    {
                        if (document.Metadata.ContainsKey(Constants.RavenReplicationConflict) &&
                            !document.Metadata.ContainsKey(Constants.RavenReplicationConflictDocument))
                        {
                            JsonDocument newDocument;
                            Database.ResolveConflict(document, actions, out newDocument);
                            if (newDocument != null)
                            {
                                document = newDocument;
                            }
                        }
                   }
                    
                });
            }

            JsonDocument.EnsureIdInMetadata(document);

            return new DocumentRetriever(null, null, Database.ReadTriggers)
                .ExecuteReadTriggers(document, transactionInformation, ReadOperation.Load);
        }

        public JsonDocumentMetadata GetDocumentMetadata(string key, TransactionInformation transactionInformation)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            key = key.Trim();
            JsonDocumentMetadata document = null;
            if (transactionInformation == null ||
                Database.InFlightTransactionalState.TryGet(key, transactionInformation, out document) == false)
            {
                TransactionalStorage.Batch(actions =>
                {
                    var nonAuthoritativeInformationBehavior = actions.InFlightStateSnapshot.GetNonAuthoritativeInformationBehavior<JsonDocumentMetadata>(transactionInformation, key);

                    document = actions.Documents.DocumentMetadataByKey(key);

                    if (nonAuthoritativeInformationBehavior != null)
                        document = nonAuthoritativeInformationBehavior(document);
                });
            }

            JsonDocument.EnsureIdInMetadata(document);
            return new DocumentRetriever(null, null, Database.ReadTriggers)
                .ProcessReadVetoes(document, transactionInformation, ReadOperation.Load);
        }

        public Etag GetLastEtagForCollection(string collectionName)
        {
            var value = Etag.Empty;
            TransactionalStorage.Batch(accessor =>
            {
                var dbvalue = accessor.Lists.Read("Raven/Collection/Etag", collectionName);
                if (dbvalue != null)
                {
                    value = Etag.Parse(dbvalue.Data.Value<byte[]>("Etag"));
                }
            });
            return value;
        }


        public JsonDocument GetWithTransformer(string key, string transformer, TransactionInformation transactionInformation, Dictionary<string, RavenJToken> transformerParameters, out HashSet<string> itemsToInclude)
        {
            JsonDocument result = null;
            DocumentRetriever docRetriever = null;
            TransactionalStorage.Batch(
                actions =>
                {
                    docRetriever = new DocumentRetriever(Database.Configuration, actions, Database.ReadTriggers, transformerParameters);
                    using (new CurrentTransformationScope(Database, docRetriever))
                    {
                        var document = Get(key, transactionInformation);
                        if (document == null)
                            return;

                        if (document.Metadata.ContainsKey("Raven-Read-Veto") || document.Metadata.ContainsKey(Constants.RavenReplicationConflict))
                        {
                            result = document;
                            return;
                        }

                        var storedTransformer = IndexDefinitionStorage.GetTransformer(transformer);
                        if (storedTransformer == null)
                            throw new InvalidOperationException("No transformer with the name: " + transformer);

                        var transformed = storedTransformer.TransformResultsDefinition(new[] {new DynamicJsonObject(document.ToJson())})
                            .Select(x => JsonExtensions.ToJObject(x))
                            .ToArray();

                        if (transformed.Length == 0)
                            return;

                        result = new JsonDocument
                        {
                            Etag = document.Etag.HashWith(storedTransformer.GetHashCodeBytes()).HashWith(docRetriever.Etag),
                            NonAuthoritativeInformation = document.NonAuthoritativeInformation,
                            LastModified = document.LastModified,
                            DataAsJson = new RavenJObject {{"$values", new RavenJArray(transformed.Cast<object>().ToArray())}}
                        };
                    }
                });
            itemsToInclude = docRetriever.ItemsToInclude;
            return result;
        }
        
        public PutResult Put(string key, Etag etag, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation, 
            IEnumerable<string> participatingIds = null,InvokeSource source = InvokeSource.Default)
        {
            WorkContext.MetricsCounters.DocsPerSecond.Mark();
            key = string.IsNullOrWhiteSpace(key) ? Guid.NewGuid().ToString() : key.Trim();
            RemoveReservedProperties(document);
            RemoveMetadataReservedProperties(metadata,source);
            var newEtag = Etag.Empty;

            using (Database.DocumentLock.Lock())
            {
                TransactionalStorage.Batch(actions =>
                {
                    if (key.EndsWith("/"))
                    {
                        key += GetNextIdentityValueWithoutOverwritingOnExistingDocuments(key, actions);
                    }
                    AssertPutOperationNotVetoed(key, metadata, document, transactionInformation);
                    if (transactionInformation == null)
                    {
                        if (Database.InFlightTransactionalState.IsModified(key))
                            throw new ConcurrencyException("PUT attempted on : " + key +
                                                           " while it is being locked by another transaction");

                        Database.PutTriggers.Apply(trigger => trigger.OnPut(key, document, metadata, null));

                        var addDocumentResult = actions.Documents.AddDocument(key, etag, document, metadata, source);
                        newEtag = addDocumentResult.Etag;

                        Database.Indexes.CheckReferenceBecauseOfDocumentUpdate(key, actions, participatingIds);

                        metadata[Constants.LastModified] = addDocumentResult.SavedAt;
                        
                        metadata.EnsureSnapshot(
                            "Metadata was written to the database, cannot modify the document after it was written (changes won't show up in the db). Did you forget to call CreateSnapshot() to get a clean copy?");
                        document.EnsureSnapshot(
                            "Document was written to the database, cannot modify the document after it was written (changes won't show up in the db). Did you forget to call CreateSnapshot() to get a clean copy?");

                        actions.AfterStorageCommitBeforeWorkNotifications(new JsonDocument
                        {
                            Metadata = metadata,
                            Key = key,
                            DataAsJson = document,
                            Etag = newEtag,
                            LastModified = addDocumentResult.SavedAt,
                            SkipDeleteFromIndex = addDocumentResult.Updated == false
                        }, documents =>
                        {
                            if (Database.IndexDefinitionStorage.IndexesCount == 0 || Database.WorkContext.RunIndexing == false)
                                return;

                            Database.Prefetcher.AfterStorageCommitBeforeWorkNotifications(PrefetchingUser.Indexer, documents);
                        });

                        Database.PutTriggers.Apply(trigger => trigger.AfterPut(key, document, metadata, newEtag, null));

                        TransactionalStorage
                            .ExecuteImmediatelyOrRegisterForSynchronization(() =>
                            {
                                Database.PutTriggers.Apply(trigger => trigger.AfterCommit(key, document, metadata, newEtag));

                                var newDocumentChangeNotification =
                                    new DocumentChangeNotification
                                    {
                                        Id = key,
                                        Type = DocumentChangeTypes.Put,
                                        TypeName = metadata.Value<string>(Constants.RavenClrType),
                                        CollectionName = metadata.Value<string>(Constants.RavenEntityName),
                                        Etag = newEtag
                                    };
                                Database.Notifications.RaiseNotifications(newDocumentChangeNotification, metadata);
                            });

                        WorkContext.ShouldNotifyAboutWork(() => "PUT " + key);
                    }
                    else
                    {
                        var doc = actions.Documents.DocumentMetadataByKey(key);
                        newEtag = Database.InFlightTransactionalState.AddDocumentInTransaction(key, etag, document, metadata,
                            transactionInformation,
                            doc == null
                                ? Etag.Empty
                                : doc.Etag,
                            UuidGenerator);
                    }
                });
                if (Log.IsDebugEnabled)
                    Log.Debug("Put document {0} with etag {1}", key, newEtag);
                return new PutResult
                {
                    Key = key,
                    ETag = newEtag
                };
            }
        }

        public bool Delete(string key, Etag etag, TransactionInformation transactionInformation, IEnumerable<string> participatingIds = null)
        {
            RavenJObject metadata;
            return Delete(key, etag, transactionInformation, out metadata, participatingIds);
        }

        public bool Delete(string key, Etag etag, TransactionInformation transactionInformation, out RavenJObject metadata, IEnumerable<string> participatingIds = null)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            key = key.Trim();

            var deleted = false;
            if (Log.IsDebugEnabled)
                Log.Debug("Delete a document with key: {0} and etag {1}", key, etag);
            RavenJObject metadataVar = null;
            using (Database.DocumentLock.Lock())
            {
                TransactionalStorage.Batch(actions =>
                {
                    AssertDeleteOperationNotVetoed(key, transactionInformation);
                    if (transactionInformation == null)
                    {
                        Database.DeleteTriggers.Apply(trigger => trigger.OnDelete(key, null));

                        string collection = null;
                        Etag deletedETag;
                        if (actions.Documents.DeleteDocument(key, etag, out metadataVar, out deletedETag))
                        {
                            deleted = true;
                            actions.Indexing.RemoveAllDocumentReferencesFrom(key);
                            WorkContext.MarkDeleted(key);

                            Database.Indexes.CheckReferenceBecauseOfDocumentUpdate(key, actions, participatingIds);

                            collection = metadataVar.Value<string>(Constants.RavenEntityName);

                            DeleteDocumentFromIndexesForCollection(key, collection, actions);
                            if (deletedETag != null)
                                Database.Prefetcher.AfterDelete(key, deletedETag);
                            Database.DeleteTriggers.Apply(trigger => trigger.AfterDelete(key, null, metadataVar));
                        }

                        TransactionalStorage
                            .ExecuteImmediatelyOrRegisterForSynchronization(() =>
                            {
                                Database.DeleteTriggers.Apply(trigger => trigger.AfterCommit(key));
                                if (string.IsNullOrEmpty(collection) == false)
                                    Database.LastCollectionEtags.Update(collection);

                                Database.Notifications.RaiseNotifications(new DocumentChangeNotification
                                {
                                    Id = key,
                                    Type = DocumentChangeTypes.Delete,
                                    TypeName = (metadataVar != null) ? metadataVar.Value<string>(Constants.RavenClrType) : null,
                                    CollectionName = (metadataVar != null) ? metadataVar.Value<string>(Constants.RavenEntityName) : null
                                }, metadataVar);
                            });
                    }
                    else
                    {
                        var doc = actions.Documents.DocumentMetadataByKey(key);

                        Database.InFlightTransactionalState.DeleteDocumentInTransaction(transactionInformation, key,
                            etag,
                            doc == null ? Etag.Empty : doc.Etag,
                            UuidGenerator);
                        deleted = doc != null;
                    }

                    WorkContext.ShouldNotifyAboutWork(() => "DEL " + key);
                });

                metadata = metadataVar;
                return deleted;
            }
        }

        internal void DeleteDocumentFromIndexesForCollection(string key, string collection, IStorageActionsAccessor actions)
        {
            foreach (var indexName in IndexDefinitionStorage.IndexNames)
            {
                var abstractViewGenerator =
                    IndexDefinitionStorage.GetViewGenerator(indexName);
                if (abstractViewGenerator == null)
                    continue;


                if (collection != null && // the document has a entity name
                    abstractViewGenerator.ForEntityNames.Count > 0)
                    // the index operations on specific entities
                {
                    if (abstractViewGenerator.ForEntityNames.Contains(collection) == false)
                        continue;
                }

                var instance = IndexDefinitionStorage.GetIndexDefinition(indexName);
                var task = actions.GetTask(x => x.Index == instance.IndexId,
                    new RemoveFromIndexTask(instance.IndexId));
                task.AddKey(key);
            }
        }
    }
}