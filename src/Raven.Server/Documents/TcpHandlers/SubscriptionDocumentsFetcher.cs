using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.TcpHandlers
{
    public class SubscriptionDocumentsFetcher
    {
        private readonly DocumentDatabase _db;
        private readonly Logger _logger;
        private readonly int _maxBatchSize;
        private readonly long _subscriptionId;
        private readonly EndPoint _remoteEndpoint;
        private readonly string _collection;
        private readonly bool _revisions;
        private readonly SubscriptionState _subscription;
        private readonly SubscriptionPatchDocument _patch;

        public SubscriptionDocumentsFetcher(DocumentDatabase db, int maxBatchSize, long subscriptionId, EndPoint remoteEndpoint, string collection,
            bool revisions,
            SubscriptionState subscription,
            SubscriptionPatchDocument patch)
        {
            _db = db;
            _logger = LoggingSource.Instance.GetLogger<SubscriptionDocumentsFetcher>(db.Name);
            _maxBatchSize = maxBatchSize;
            _subscriptionId = subscriptionId;
            _remoteEndpoint = remoteEndpoint;
            _collection = collection;
            _revisions = revisions;
            _subscription = subscription;
            _patch = patch;
        }

        public IEnumerable<(Document Doc, Exception Exception)> GetDataToSend(
            DocumentsOperationContext docsContext, 
            long startEtag)
        {
            if (string.IsNullOrEmpty(_collection))
                throw new ArgumentException("The collection name must be specified");

            if (_revisions)
            {
                if (_db.DocumentsStorage.RevisionsStorage.Configuration == null ||
                    _db.DocumentsStorage.RevisionsStorage.GetRevisionsConfiguration(_collection).Disabled)
                    throw new SubscriptionInvalidStateException($"Cannot use a revisions subscription, database {_db.Name} does not have revisions configuration.");

                return GetRevisionsToSend(docsContext, startEtag);
            }


            return GetDocumentsToSend(docsContext, startEtag);
        }

        private IEnumerable<(Document Doc, Exception Exception)> GetDocumentsToSend(DocumentsOperationContext docsContext, 
            long startEtag)
        {
            int size = 0;
            using (_db.Scripts.GetScriptRunner(_patch,true, out var run))
            {
                foreach (var doc in _db.DocumentsStorage.GetDocumentsFrom(
                    docsContext,
                    _collection,
                    startEtag + 1,
                    0,
                    int.MaxValue))
                {
                    using (doc.Data)
                    {
                        if (ShouldSendDocument(_subscription, run, _patch, docsContext, doc, out BlittableJsonReaderObject transformResult, out var exception) == false)
                        {
                            if (exception != null)
                            {
                                yield return (doc, exception);
                                if (++size >= _maxBatchSize)
                                    yield break;
                            }
                            else
                            {
                                doc.Data = null;
                                yield return (doc, null);
                            }
                            doc.Data = null;
                        }
                        else
                        {
                            using (transformResult)
                            {
                                if (transformResult == null)
                                {
                                    yield return (doc, null);

                                }
                                else
                                {
                                    var projection = new Document
                                    {
                                        Id = doc.Id,
                                        Etag = doc.Etag,
                                        Data = transformResult,
                                        LowerId = doc.LowerId,
                                        ChangeVector = doc.ChangeVector,
                                        LastModified = doc.LastModified,
                                    };

                                    yield return (projection, null);
                                }
                            }
                            if (++size >= _maxBatchSize)
                                yield break;
                        }
                    }
                }
            }
        }

        private IEnumerable<(Document Doc, Exception Exception)> GetRevisionsToSend(
            DocumentsOperationContext docsContext, 
            long startEtag)
        {
            int size = 0;
            
            var collectionName = new CollectionName(_collection);
            using (_db.Scripts.GetScriptRunner(_patch, true, out var run))
            {
                foreach (var revisionTuple in _db.DocumentsStorage.RevisionsStorage.GetRevisionsFrom(docsContext, collectionName, startEtag + 1, int.MaxValue))
                {
                    var item = (revisionTuple.current ?? revisionTuple.previous);
                    Debug.Assert(item != null);

                    if (ShouldSendDocumentWithRevisions(_subscription, run, _patch, docsContext, item, revisionTuple, out var transformResult, out var exception) == false)
                    {
                        if (exception != null)
                        {
                            yield return (item, exception);
                            if (++size >= _maxBatchSize)
                                yield break;
                        }
                        else
                        {
                            // make sure that if we read a lot of irrelevant documents, we send keep alive over the network
                            yield return (new Document
                            {
                                Data = null,
                                ChangeVector = item.ChangeVector,
                                Etag = item.Etag,
                                LastModified = item.LastModified,
                            }, null);
                        }
                    }
                    else
                    {
                        using (transformResult)
                        {
                            if (transformResult == null)
                            {
                                yield return (revisionTuple.current, null);
                            }
                            else
                            {
                                var projection = new Document
                                {
                                    Id = item.Id,
                                    Etag = item.Etag,
                                    Data = transformResult,
                                    LowerId = item.LowerId,
                                    ChangeVector = item.ChangeVector,
                                    LastModified = item.LastModified,
                                };

                                yield return (projection, null);
                            }
                            if (++size >= _maxBatchSize)
                                yield break;
                        }
                    }
                }
            }
        }

        private bool ShouldSendDocument(SubscriptionState subscriptionState,
            ScriptRunner.SingleRun run,
            SubscriptionPatchDocument patch,
            DocumentsOperationContext dbContext,
            Document doc,
            out BlittableJsonReaderObject transformResult,
            out Exception exception)
        {
            transformResult = null;
            exception = null;
            var conflictStatus = ChangeVectorUtils.GetConflictStatus(
                remoteAsString: doc.ChangeVector,
                localAsString: subscriptionState.ChangeVectorForNextBatchStartingPoint);

            if (conflictStatus == ConflictStatus.AlreadyMerged)
                return false;

            if (patch == null)
                return true;

            try
            {
                return patch.MatchCriteria(run, dbContext, doc, ProjectionMetadataModifier.Instance, ref transformResult);
            }
            catch (Exception ex)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info(
                        $"Criteria script threw exception for subscription {_subscriptionId} connected to {_remoteEndpoint} for document id {doc.Id}",
                        ex);
                }
                exception = ex;
                return false;
            }
        }

        private bool ShouldSendDocumentWithRevisions(SubscriptionState subscriptionState,
            ScriptRunner.SingleRun run,
            SubscriptionPatchDocument patch,
            DocumentsOperationContext dbContext,
            Document item,
            (Document Previous, Document Current) revision,
            out BlittableJsonReaderObject transformResult,
            out Exception exception)
        {
            exception = null;
            transformResult = null;
            var conflictStatus = ChangeVectorUtils.GetConflictStatus(
                remoteAsString: item.ChangeVector,
                localAsString: subscriptionState.ChangeVectorForNextBatchStartingPoint);

            if (conflictStatus == ConflictStatus.AlreadyMerged)
                return false;

            revision.Current?.EnsureMetadata();
            revision.Previous?.EnsureMetadata();

            transformResult = dbContext.ReadObject(new DynamicJsonValue
            {
                ["Current"] = revision.Current?.Data,
                ["Previous"] = revision.Previous?.Data
            }, item.Id);


            if (patch == null)
                return true;

            revision.Current?.ResetModifications();
            revision.Previous?.ResetModifications();
         
            try
            {
                return patch.MatchCriteria(run, dbContext, transformResult, ProjectionMetadataModifier.Instance, ref transformResult);
            }
            catch (Exception ex)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info(
                        $"Criteria script threw exception for subscription {_subscriptionId} connected to {_remoteEndpoint} for document id {item.Id}",
                        ex);
                }
                exception = ex;
                return false;
            }
        }

        private class ProjectionMetadataModifier : JsBlittableBridge.IResultModifier
        {
            public static readonly ProjectionMetadataModifier Instance = new ProjectionMetadataModifier();

            private ProjectionMetadataModifier()
            {
            }

            public void Modify(ObjectInstance json)
            {
                ObjectInstance metadata;
                var value = json.Get(Constants.Documents.Metadata.Key);
                if (value.Type == Types.Object)
                    metadata = value.AsObject();
                else
                {
                    metadata = json.Engine.Object.Construct(Array.Empty<JsValue>());
                    json.Put(Constants.Documents.Metadata.Key, metadata, false);
                }

                metadata.Put(Constants.Documents.Metadata.Projection, JsValue.True, false);
            }
        }
    }
}
