using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
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

        public SubscriptionDocumentsFetcher(DocumentDatabase db, int maxBatchSize, long subscriptionId, EndPoint remoteEndpoint)
        {
            _db = db;
            _logger = LoggingSource.Instance.GetLogger<SubscriptionDocumentsFetcher>(db.Name);
            _maxBatchSize = maxBatchSize;
            _subscriptionId = subscriptionId;
            _remoteEndpoint = remoteEndpoint;
        }

        public IEnumerable<(Document Doc, Exception Exception)> GetDataToSend(DocumentsOperationContext docsContext, SubscriptionState subscription, SubscriptionPatchDocument patch, long startEtag)
        {
            if (string.IsNullOrEmpty(subscription.Criteria?.Collection))
                throw new ArgumentException("The collection name must be specified");

            if (subscription.Criteria.IncludeRevisions)
            {
                if (_db.DocumentsStorage.RevisionsStorage.Configuration == null ||
                    _db.DocumentsStorage.RevisionsStorage.GetRevisionsConfiguration(subscription.Criteria.Collection).Active == false)
                    throw new SubscriptionInvalidStateException($"Cannot use a revisions subscription, database {_db.Name} does not have revisions configuration.");

                return GetRevisionsToSend(docsContext, subscription, startEtag, patch);
            }


            return GetDocumentsToSend(docsContext, subscription, startEtag, patch);
        }

        private IEnumerable<(Document Doc, Exception Exception)> GetDocumentsToSend(DocumentsOperationContext docsContext, SubscriptionState subscription,
            long startEtag, SubscriptionPatchDocument patch)
        {
            using (_db.Scripts.GetScriptRunner(patch?.Key, out var run))
            {
                foreach (var doc in _db.DocumentsStorage.GetDocumentsFrom(
                    docsContext,
                    subscription.Criteria.Collection,
                    startEtag + 1,
                    0,
                    _maxBatchSize))
                {
                    using (doc.Data)
                    {
                        if (ShouldSendDocument(subscription, run, patch, docsContext, doc, out BlittableJsonReaderObject transformResult, out var exception) == false)
                        {
                            if (exception != null)
                            {
                                yield return (doc, exception);
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
                                    continue;
                                }

                                yield return (new Document
                                {
                                    Id = doc.Id,
                                    Etag = doc.Etag,
                                    Data = transformResult,
                                    LowerId = doc.LowerId,
                                    ChangeVector = doc.ChangeVector
                                }, null);
                            }
                        }
                    }
                }
            }
        }

        private IEnumerable<(Document Doc, Exception Exception)> GetRevisionsToSend(DocumentsOperationContext docsContext, SubscriptionState subscription,
            long startEtag, SubscriptionPatchDocument patch)
        {
            var collectionName = new CollectionName(subscription.Criteria.Collection);
            using (_db.Scripts.GetScriptRunner(patch?.Key, out var run))
            {
                foreach (var revisionTuple in _db.DocumentsStorage.RevisionsStorage.GetRevisionsFrom(docsContext, collectionName, startEtag + 1, _maxBatchSize))
                {
                    var item = (revisionTuple.current ?? revisionTuple.previous);
                    Debug.Assert(item != null);

                    var dynamicValue = new DynamicJsonValue();

                    if (revisionTuple.current != null)
                        dynamicValue["Current"] = revisionTuple.current.Data;

                    if (revisionTuple.previous != null)
                        dynamicValue["Previous"] = revisionTuple.previous.Data;

                    using (var revision = docsContext.ReadObject(dynamicValue, item.Id))
                    {
                        if (ShouldSendDocumentWithRevisions(subscription, run, patch, docsContext, item, revision, out var transformResult, out var exception) == false)
                        {
                            if (exception != null)
                            {
                                yield return (revisionTuple.current, exception);
                            }
                            else
                            {
                                // make sure that if we read a lot of irrelevant documents, we send keep alive over the network
                                yield return (new Document
                                {
                                    Data = null,
                                    ChangeVector = item.ChangeVector,
                                    Etag = item.Etag
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
                                    continue;
                                }

                                yield return (new Document
                                {
                                    Id = item.Id,
                                    Etag = item.Etag,
                                    Data = transformResult,
                                    LowerId = item.LowerId,
                                    ChangeVector = item.ChangeVector
                                }, null);
                            }
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
                return patch.MatchCriteria(run, dbContext, doc, out transformResult);
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
            BlittableJsonReaderObject revision,
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

            if (patch == null)
                return true;

            if (patch.FilterJavaScript == SubscriptionCreationOptions.DefaultRevisionsScript)
            {
                transformResult = revision;
                return true;
            }
            try
            {
                var docToProccess = new Document
                {
                    Data = revision,
                    Id = item.Id
                };

                return patch.MatchCriteria(run, dbContext, docToProccess, out transformResult);
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




    }
}
