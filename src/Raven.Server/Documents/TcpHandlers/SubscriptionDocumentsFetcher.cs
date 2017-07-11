using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using Raven.Client.Documents.Exceptions.Subscriptions;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Versioning;
using Raven.Server.ServerWide.Context;
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

        public IEnumerable<(Document Doc,Exception Exception)> GetDataToSend(DocumentsOperationContext docsContext, SubscriptionState subscription, long startEtag, SubscriptionPatchDocument patch)
        {
            if (string.IsNullOrEmpty(subscription.Criteria?.Collection))
                throw new ArgumentException("The collection name must be specified");

            if (subscription.Criteria.IsVersioned)
            {
                if (_db.DocumentsStorage.VersioningStorage == null || _db.DocumentsStorage.VersioningStorage.IsVersioned(subscription.Criteria.Collection) == false)
                    throw new SubscriptionInvalidStateException($"Cannot use a versioned subscription, database {_db.Name} does not have versioning setup"); 

                return GetVerionTuplesToSend(docsContext, subscription, startEtag, patch, _db.DocumentsStorage.VersioningStorage);
            }


            return GetDocumentsToSend(docsContext, subscription, startEtag, patch, _db);
        }

        private IEnumerable<(Document Doc, Exception Exception)> GetDocumentsToSend(DocumentsOperationContext docsContext, SubscriptionState subscription, long startEtag, SubscriptionPatchDocument patch,
            DocumentDatabase db)
        {
            foreach (var doc in db.DocumentsStorage.GetDocumentsFrom(
                docsContext,
                subscription.Criteria.Collection,
                startEtag + 1,
                0,
                _maxBatchSize))
            {
                using (doc.Data)
                {
                    BlittableJsonReaderObject transformResult;
                    if (ShouldSendDocument(subscription, patch, docsContext, doc, out transformResult, out var exception) == false)
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
                                yield return (doc,null);
                                continue;
                            }

                            yield return (new Document
                            {
                                Id = doc.Id,
                                Etag = doc.Etag,
                                Data = transformResult,
                                LowerId = doc.LowerId,
                                ChangeVector = doc.ChangeVector
                            },null);
                        }
                    }
                }
            }
        }

        private IEnumerable<(Document Doc, Exception Exception)> GetVerionTuplesToSend(DocumentsOperationContext docsContext, SubscriptionState subscription, long startEtag, SubscriptionPatchDocument patch,
            VersioningStorage revisions)
        {
            foreach (var versionedDocs in revisions.GetRevisionsFrom(docsContext, new CollectionName(subscription.Criteria.Collection), startEtag + 1, _maxBatchSize))
            {
                var item = (versionedDocs.current ?? versionedDocs.previous);
                Debug.Assert(item != null);

                var dynamicValue = new DynamicJsonValue();

                if (versionedDocs.current != null)
                    dynamicValue["Current"] = versionedDocs.current.Data;

                if (versionedDocs.previous != null)
                    dynamicValue["Previous"] = versionedDocs.previous.Data;

                using (var versioned = docsContext.ReadObject(dynamicValue, item.Id))
                {
                    if (ShouldSendDocumentWithVersioning(subscription, patch, docsContext, item, versioned, out var transformResult, out var exception) == false)
                    {
                        if (exception != null)
                        {
                            yield return (versionedDocs.current, exception);
                        }
                        else
                        {
                            // make sure that if we read a lot of irrelevant documents, we send keep alive over the network
                            yield return (new Document
                            {
                                Data = null,
                                ChangeVector = item.ChangeVector,
                                Etag = item.Etag
                            }, null );
                        }
                    }
                    else
                    {
                        using (transformResult)
                        {
                            if (transformResult == null)
                            {
                                yield return (versionedDocs.current,null);
                                continue;
                            }

                            yield return (new Document
                            {
                                Id = item.Id,
                                Etag = item.Etag,
                                Data = transformResult,
                                LowerId = item.LowerId,
                                ChangeVector = item.ChangeVector
                            },null);
                        }
                    }
                }
            }
        }
        
        private bool ShouldSendDocument(SubscriptionState subscriptionState, SubscriptionPatchDocument patch, DocumentsOperationContext dbContext,
            Document doc, out BlittableJsonReaderObject transformResult, out Exception exception)
        {
            transformResult = null;
            exception = null;
            var conflictStatus = ConflictsStorage.GetConflictStatus(
                remote: doc.ChangeVector,
                local: subscriptionState.ChangeVector.ToChangeVector());

            if (conflictStatus == ConflictsStorage.ConflictStatus.AlreadyMerged)
                return false;

            if (patch == null)
                return true;

            try
            {
                return patch.MatchCriteria(dbContext, doc, out transformResult);
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
        
        
        private bool ShouldSendDocumentWithVersioning(SubscriptionState subscriptionState, SubscriptionPatchDocument patch, DocumentsOperationContext dbContext,
            Document item, BlittableJsonReaderObject versioned, out BlittableJsonReaderObject transformResult, out Exception exception)
        {
            exception = null;
            transformResult = null;
            var conflictStatus = ConflictsStorage.GetConflictStatus(
                remote: item.ChangeVector,
                local: subscriptionState.ChangeVector.ToChangeVector());

            if (conflictStatus == ConflictsStorage.ConflictStatus.AlreadyMerged)
                return false;

            if (patch == null)
                return true;
            
            if (patch.FilterJavaScript == SubscriptionCreationOptions.DefaultVersioningScript)
            {
                transformResult = versioned;
                return true;
            }
            try
            {
                var docToProccess = new Document
                {
                    Data = versioned,
                    Id = item.Id,
                };

                return patch.MatchCriteria(dbContext, docToProccess, out transformResult);
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