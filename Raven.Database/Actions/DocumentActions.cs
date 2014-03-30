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
using Raven.Database.Data;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Linq;
using Raven.Database.Plugins;
using Raven.Database.Prefetching;
using Raven.Database.Storage;
using Raven.Database.Tasks;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Actions
{
    public class DocumentActions : ActionsBase
    {
        public DocumentActions(DocumentDatabase database, SizeLimitedConcurrentDictionary<string, TouchedDocumentInfo> recentTouches, IUuidGenerator uuidGenerator, ILog log)
            : base(database, recentTouches, uuidGenerator, log)
        {
        }

        public long GetNextIdentityValueWithoutOverwritingOnExistingDocuments(string key,
    IStorageActionsAccessor actions,
    TransactionInformation transactionInformation)
        {
            int tries;
            return GetNextIdentityValueWithoutOverwritingOnExistingDocuments(key, actions, transactionInformation, out tries);
        }

        public long GetNextIdentityValueWithoutOverwritingOnExistingDocuments(string key,
            IStorageActionsAccessor actions,
            TransactionInformation transactionInformation,
            out int tries)
        {
            long nextIdentityValue = actions.General.GetNextIdentityValue(key);

            if (actions.Documents.DocumentMetadataByKey(key + nextIdentityValue, transactionInformation) == null)
            {
                tries = 1;
                return nextIdentityValue;
            }
            tries = 1;
            // there is already a document with this id, this means that we probably need to search
            // for an opening in potentially large data set. 
            var lastKnownBusy = nextIdentityValue;
            var maybeFree = nextIdentityValue * 2;
            var lastKnownFree = long.MaxValue;
            while (true)
            {
                tries++;
                if (actions.Documents.DocumentMetadataByKey(key + maybeFree, transactionInformation) == null)
                {
                    if (lastKnownBusy + 1 == maybeFree)
                    {
                        actions.General.SetIdentityValue(key, maybeFree);
                        return maybeFree;
                    }
                    lastKnownFree = maybeFree;
                    maybeFree = Math.Max(maybeFree - (maybeFree - lastKnownBusy) / 2, lastKnownBusy + 1);

                }
                else
                {
                    lastKnownBusy = maybeFree;
                    maybeFree = Math.Min(lastKnownFree, maybeFree * 2);
                }
            }
        }


        private void AssertPutOperationNotVetoed(string key, RavenJObject metadata, RavenJObject document, TransactionInformation transactionInformation)
        {
            var vetoResult = Database.PutTriggers
                .Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowPut(key, document, metadata, transactionInformation) })
                .FirstOrDefault(x => x.VetoResult.IsAllowed == false);
            if (vetoResult != null)
            {
                throw new OperationVetoedException("PUT vetoed on document " + key + " by " + vetoResult.Trigger + " because: " + vetoResult.VetoResult.Reason);
            }
        }

        public RavenJArray GetDocumentsWithIdStartingWith(string idPrefix, string matches, string exclude, int start,
                                                          int pageSize, CancellationToken token, ref int nextStart,
                                                          string transformer = null, Dictionary<string, RavenJToken> queryInputs = null)
        {
            var list = new RavenJArray();
            GetDocumentsWithIdStartingWith(idPrefix, matches, exclude, start, pageSize, token, ref nextStart, list.Add,
                                           transformer, queryInputs);
            return list;
        }

        public void GetDocumentsWithIdStartingWith(string idPrefix, string matches, string exclude, int start, int pageSize,
                                                   CancellationToken token, ref int nextStart, Action<RavenJObject> addDoc,
                                                   string transformer = null, Dictionary<string, RavenJToken> queryInputs = null)
        {
            if (idPrefix == null)
                throw new ArgumentNullException("idPrefix");
            idPrefix = idPrefix.Trim();

            var canPerformRapidPagination = nextStart > 0 && start == nextStart;
            var actualStart = canPerformRapidPagination ? start : 0;
            var addedDocs = 0;
            var matchedDocs = 0;

            TransactionalStorage.Batch(
                actions =>
                {
                    var docsToSkip = canPerformRapidPagination ? 0 : start;
                    int docCount;

                    AbstractTransformer storedTransformer = null;
                    if (transformer != null)
                    {
                        storedTransformer = IndexDefinitionStorage.GetTransformer(transformer);
                        if (storedTransformer == null)
                            throw new InvalidOperationException("No transformer with the name: " + transformer);
                    }

                    do
                    {
                        docCount = 0;
                        var docs = actions.Documents.GetDocumentsWithIdStartingWith(idPrefix, actualStart, pageSize);
                        var documentRetriever = new DocumentRetriever(actions, Database.ReadTriggers, Database.InFlightTransactionalState, queryInputs);

                        foreach (var doc in docs)
                        {
                            token.ThrowIfCancellationRequested();
                            docCount++;
                            var keyTest = doc.Key.Substring(idPrefix.Length);

                            if (!WildcardMatcher.Matches(matches, keyTest) || WildcardMatcher.MatchesExclusion(exclude, keyTest))
                                continue;

                            DocumentRetriever.EnsureIdInMetadata(doc);
                            var nonAuthoritativeInformationBehavior = Database.InFlightTransactionalState.GetNonAuthoritativeInformationBehavior<JsonDocument>(null, doc.Key);

                            var document = nonAuthoritativeInformationBehavior != null ? nonAuthoritativeInformationBehavior(doc) : doc;
                            document = documentRetriever.ExecuteReadTriggers(document, null, ReadOperation.Load);
                            if (document == null)
                                continue;

                            matchedDocs++;

                            if (matchedDocs <= docsToSkip)
                                continue;

                            token.ThrowIfCancellationRequested();

                            if (storedTransformer != null)
                            {
                                using (new CurrentTransformationScope(documentRetriever))
                                {
                                    var transformed =
                                        storedTransformer.TransformResultsDefinition(new[] { new DynamicJsonObject(document.ToJson()) })
                                                         .Select(x => JsonExtensions.ToJObject(x))
                                                         .ToArray();

                                    if (transformed.Length == 0)
                                    {
                                        throw new InvalidOperationException("The transform results function failed on a document: " + document.Key);
                                    }

                                    var transformedJsonDocument = new JsonDocument
                                    {
                                        Etag = document.Etag.HashWith(storedTransformer.GetHashCodeBytes()).HashWith(documentRetriever.Etag),
                                        NonAuthoritativeInformation = document.NonAuthoritativeInformation,
                                        LastModified = document.LastModified,
                                        DataAsJson = new RavenJObject { { "$values", new RavenJArray(transformed) } },
                                    };

                                    addDoc(transformedJsonDocument.ToJson());
                                }

                            }
                            else
                            {
                                addDoc(document.ToJson());
                            }

                            addedDocs++;

                            if (addedDocs >= pageSize)
                                break;
                        }

                        actualStart += pageSize;
                    }
                    while (docCount > 0 && addedDocs < pageSize && actualStart > 0 && actualStart < int.MaxValue);
                });

            if (addedDocs != pageSize)
                nextStart = start; // will mark as last page
            else if (canPerformRapidPagination)
                nextStart = start + matchedDocs;
            else
                nextStart = actualStart;
        }

        private static void RemoveMetadataReservedProperties(RavenJObject metadata)
        {
            RemoveReservedProperties(metadata);
            metadata.Remove("Raven-Last-Modified");
            metadata.Remove("Last-Modified");
        }

        private static void RemoveReservedProperties(RavenJObject document)
        {
            document.Remove(string.Empty);
            var toRemove = document.Keys.Where(propertyName => propertyName.StartsWith("@") || HeadersToIgnoreServer.Contains(propertyName)).ToList();
            foreach (var propertyName in toRemove)
            {
                document.Remove(propertyName);
            }
        }

        private void AssertDeleteOperationNotVetoed(string key, TransactionInformation transactionInformation)
        {
            var vetoResult = Database.DeleteTriggers
                .Select(trigger => new { Trigger = trigger, VetoResult = trigger.AllowDelete(key, transactionInformation) })
                .FirstOrDefault(x => x.VetoResult.IsAllowed == false);
            if (vetoResult != null)
            {
                throw new OperationVetoedException("DELETE vetoed on document " + key + " by " + vetoResult.Trigger +
                                                   " because: " + vetoResult.VetoResult.Reason);
            }
        }

        public int BulkInsert(BulkInsertOptions options, IEnumerable<IEnumerable<JsonDocument>> docBatches, Guid operationId)
        {
            var documents = 0;
            TransactionalStorage.Batch(accessor =>
            {
                Database.Notifications.RaiseNotifications(new BulkInsertChangeNotification
                {
                    OperationId = operationId,
                    Type = DocumentChangeTypes.BulkInsertStarted
                });
                foreach (var docs in docBatches)
                {
                    WorkContext.CancellationToken.ThrowIfCancellationRequested();

                    using (Database.DocumentLock.Lock())
                    {
                        var inserts = 0;
                        var batch = 0;
                        var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                        var docsToInsert = docs.ToArray();

                        foreach (var doc in docsToInsert)
                        {
                            try
                            {
                                RemoveReservedProperties(doc.DataAsJson);
                                RemoveMetadataReservedProperties(doc.Metadata);

                                if (options.CheckReferencesInIndexes)
                                    keys.Add(doc.Key);
                                documents++;
                                batch++;
                                AssertPutOperationNotVetoed(doc.Key, doc.Metadata, doc.DataAsJson, null);
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


                                foreach (var trigger in Database.PutTriggers)
                                {
                                    trigger.Value.AfterPut(doc.Key, doc.DataAsJson, doc.Metadata, result.Etag, null);
                                }
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
                        {
                            foreach (var key in keys)
                            {
                                Database.Indexes.CheckReferenceBecauseOfDocumentUpdate(key, accessor);
                            }
                        }

                        accessor.Documents.IncrementDocumentCount(inserts);
                        accessor.General.PulseTransaction();

                        WorkContext.ShouldNotifyAboutWork(() => "BulkInsert batch of " + batch + " docs");
                        WorkContext.NotifyAboutWork(); // forcing notification so we would start indexing right away
                    }
                }

                Database.Notifications.RaiseNotifications(new BulkInsertChangeNotification
                {
                    OperationId = operationId,
                    Type = DocumentChangeTypes.BulkInsertEnded
                });
                if (documents == 0)
                    return;
                WorkContext.ShouldNotifyAboutWork(() => "BulkInsert of " + documents + " docs");
            });
            return documents;
        }

        public TouchedDocumentInfo GetRecentTouchesFor(string key)
        {
            TouchedDocumentInfo info;
            RecentTouches.TryGetValue(key, out info);
            return info;
        }

        public RavenJArray GetDocuments(int start, int pageSize, Etag etag, CancellationToken token)
        {
            var list = new RavenJArray();
            GetDocuments(start, pageSize, etag, token, list.Add);
            return list;
        }

        public void GetDocuments(int start, int pageSize, Etag etag, CancellationToken token, Action<RavenJObject> addDocument)
        {
            TransactionalStorage.Batch(actions =>
            {
                bool returnedDocs = false;
                while (true)
                {
                    var documents = etag == null
                                        ? actions.Documents.GetDocumentsByReverseUpdateOrder(start, pageSize)
                                        : actions.Documents.GetDocumentsAfter(etag, pageSize);
                    var documentRetriever = new DocumentRetriever(actions, Database.ReadTriggers, Database.InFlightTransactionalState);
                    int docCount = 0;
                    foreach (var doc in documents)
                    {
                        docCount++;
                        token.ThrowIfCancellationRequested();
                        if (etag != null)
                            etag = doc.Etag;
                        DocumentRetriever.EnsureIdInMetadata(doc);
                        var nonAuthoritativeInformationBehavior = Database.InFlightTransactionalState.GetNonAuthoritativeInformationBehavior<JsonDocument>(null, doc.Key);
                        var document = nonAuthoritativeInformationBehavior == null ? doc : nonAuthoritativeInformationBehavior(doc);
                        document = documentRetriever
                            .ExecuteReadTriggers(document, null, ReadOperation.Load);
                        if (document == null)
                            continue;

                        addDocument(document.ToJson());
                        returnedDocs = true;
                    }
                    if (returnedDocs || docCount == 0)
                        break;
                    start += docCount;
                }
            });
        }




        public JsonDocument Get(string key, TransactionInformation transactionInformation)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            key = key.Trim();

            JsonDocument document = null;
            if (transactionInformation == null ||
                Database.InFlightTransactionalState.TryGet(key, transactionInformation, out document) == false)
            {
                // first we check the dtc state, then the storage, to avoid race conditions
                var nonAuthoritativeInformationBehavior = Database.InFlightTransactionalState.GetNonAuthoritativeInformationBehavior<JsonDocument>(transactionInformation, key);

                TransactionalStorage.Batch(actions => { document = actions.Documents.DocumentByKey(key, transactionInformation); });

                if (nonAuthoritativeInformationBehavior != null)
                    document = nonAuthoritativeInformationBehavior(document);
            }

            DocumentRetriever.EnsureIdInMetadata(document);

            return new DocumentRetriever(null, Database.ReadTriggers, Database.InFlightTransactionalState)
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
                var nonAuthoritativeInformationBehavior = Database.InFlightTransactionalState.GetNonAuthoritativeInformationBehavior<JsonDocumentMetadata>(transactionInformation, key);
                TransactionalStorage.Batch(actions =>
                {
                    document = actions.Documents.DocumentMetadataByKey(key, transactionInformation);
                });
                if (nonAuthoritativeInformationBehavior != null)
                    document = nonAuthoritativeInformationBehavior(document);
            }

            DocumentRetriever.EnsureIdInMetadata(document);
            return new DocumentRetriever(null, Database.ReadTriggers, Database.InFlightTransactionalState)
                .ProcessReadVetoes(document, transactionInformation, ReadOperation.Load);
        }

        public Etag GetLastEtagForCollection(string collectionName)
        {
            Etag value = Etag.Empty;
            TransactionalStorage.Batch(accessor =>
            {
                var dbvalue = accessor.Lists.Read("Raven/Collection/Etag", collectionName);
                if (dbvalue != null)
                {
                    value = Etag.Parse(dbvalue.Data.Value<Byte[]>("Etag"));
                }
            });
            return value;
        }


        public JsonDocument GetWithTransformer(string key, string transformer, TransactionInformation transactionInformation, Dictionary<string, RavenJToken> queryInputs)
        {
            JsonDocument result = null;
            TransactionalStorage.Batch(
            actions =>
            {
                var docRetriever = new DocumentRetriever(actions, Database.ReadTriggers, Database.InFlightTransactionalState, queryInputs);
                using (new CurrentTransformationScope(docRetriever))
                {
                    var document = Get(key, transactionInformation);
                    if (document == null)
                        return;

                    if (document.Metadata.ContainsKey("Raven-Read-Veto"))
                    {
                        result = document;
                        return;
                    }

                    var storedTransformer = IndexDefinitionStorage.GetTransformer(transformer);
                    if (storedTransformer == null)
                        throw new InvalidOperationException("No transformer with the name: " + transformer);

                    var transformed = storedTransformer.TransformResultsDefinition(new[] { new DynamicJsonObject(document.ToJson()) })
                                     .Select(x => JsonExtensions.ToJObject(x))
                                     .ToArray();

                    if (transformed.Length == 0)
                        return;

                    result = new JsonDocument
                    {
                        Etag = document.Etag.HashWith(storedTransformer.GetHashCodeBytes()).HashWith(docRetriever.Etag),
                        NonAuthoritativeInformation = document.NonAuthoritativeInformation,
                        LastModified = document.LastModified,
                        DataAsJson = new RavenJObject { { "$values", new RavenJArray(transformed) } },
                    };
                }
            });
            return result;
        }



        public PutResult Put(string key, Etag etag, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
        {
            WorkContext.MetricsCounters.DocsPerSecond.Mark();
            key = string.IsNullOrWhiteSpace(key) ? Guid.NewGuid().ToString() : key.Trim();
            RemoveReservedProperties(document);
            RemoveMetadataReservedProperties(metadata);
            Etag newEtag = Etag.Empty;

            using (Database.DocumentLock.Lock())
            {
                TransactionalStorage.Batch(actions =>
                {
                    if (key.EndsWith("/"))
                    {
                        key += GetNextIdentityValueWithoutOverwritingOnExistingDocuments(key, actions,
                                                                                         transactionInformation);
                    }
                    AssertPutOperationNotVetoed(key, metadata, document, transactionInformation);
                    if (transactionInformation == null)
                    {
                        if (Database.InFlightTransactionalState.IsModified(key))
                            throw new ConcurrencyException("PUT attempted on : " + key +
                                                           " while it is being locked by another transaction");

                        Database.PutTriggers.Apply(trigger => trigger.OnPut(key, document, metadata, null));

                        var addDocumentResult = actions.Documents.AddDocument(key, etag, document, metadata);
                        newEtag = addDocumentResult.Etag;

                        Database.Indexes.CheckReferenceBecauseOfDocumentUpdate(key, actions);
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
                            Database.LastCollectionEtags.UpdatePerCollectionEtags(documents);
                            Database.Prefetcher.AfterStorageCommitBeforeWorkNotifications(PrefetchingUser.Indexer, documents);
                        });

                        if (addDocumentResult.Updated)
                            Database.Prefetcher.AfterUpdate(key, addDocumentResult.PrevEtag);

                        Database.PutTriggers.Apply(trigger => trigger.AfterPut(key, document, metadata, newEtag, null));

                        TransactionalStorage
                            .ExecuteImmediatelyOrRegisterForSynchronization(() =>
                            {
                                Database.PutTriggers.Apply(trigger => trigger.AfterCommit(key, document, metadata, newEtag));
                                Database.Notifications.RaiseNotifications(new DocumentChangeNotification
                                {
                                    Id = key,
                                    Type = DocumentChangeTypes.Put,
                                    TypeName = metadata.Value<string>(Constants.RavenClrType),
                                    CollectionName = metadata.Value<string>(Constants.RavenEntityName),
                                    Etag = newEtag,
                                }, metadata);
                            });

                        WorkContext.ShouldNotifyAboutWork(() => "PUT " + key);
                    }
                    else
                    {
                        var doc = actions.Documents.DocumentMetadataByKey(key, null);
                        newEtag = Database.InFlightTransactionalState.AddDocumentInTransaction(key, etag, document, metadata,
                                                                                      transactionInformation,
                                                                                      doc == null
                                                                                          ? Etag.Empty
                                                                                          : doc.Etag,
                                                                                      UuidGenerator);
                    }
                });

                Log.Debug("Put document {0} with etag {1}", key, newEtag);
                return new PutResult
                {
                    Key = key,
                    ETag = newEtag
                };
            }
        }

        public bool Delete(string key, Etag etag, TransactionInformation transactionInformation)
        {
            RavenJObject metadata;
            return Delete(key, etag, transactionInformation, out metadata);
        }

        public bool Delete(string key, Etag etag, TransactionInformation transactionInformation, out RavenJObject metadata)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            key = key.Trim();

            var deleted = false;
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

                        Etag deletedETag;
                        if (actions.Documents.DeleteDocument(key, etag, out metadataVar, out deletedETag))
                        {
                            deleted = true;
                            actions.Indexing.RemoveAllDocumentReferencesFrom(key);
                            WorkContext.MarkDeleted(key);

                            Database.Indexes.CheckReferenceBecauseOfDocumentUpdate(key, actions);

                            foreach (var indexName in IndexDefinitionStorage.IndexNames)
                            {
                                AbstractViewGenerator abstractViewGenerator =
                                    IndexDefinitionStorage.GetViewGenerator(indexName);
                                if (abstractViewGenerator == null)
                                    continue;

                                var token = metadataVar.Value<string>(Constants.RavenEntityName);

                                if (token != null && // the document has a entity name
                                    abstractViewGenerator.ForEntityNames.Count > 0)
                                // the index operations on specific entities
                                {
                                    if (abstractViewGenerator.ForEntityNames.Contains(token) == false)
                                        continue;
                                }

                                var instance = IndexDefinitionStorage.GetIndexDefinition(indexName);
                                var task = actions.GetTask(x => x.Index == instance.IndexId, new RemoveFromIndexTask
                                {
                                    Index = instance.IndexId
                                });
                                task.Keys.Add(key);
                            }
                            if (deletedETag != null)
                                Database.Prefetcher.AfterDelete(key, deletedETag);
                            Database.DeleteTriggers.Apply(trigger => trigger.AfterDelete(key, null));
                        }

                        TransactionalStorage
                            .ExecuteImmediatelyOrRegisterForSynchronization(() =>
                            {
                                Database.DeleteTriggers.Apply(trigger => trigger.AfterCommit(key));
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
                        var doc = actions.Documents.DocumentMetadataByKey(key, null);

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

    }
}