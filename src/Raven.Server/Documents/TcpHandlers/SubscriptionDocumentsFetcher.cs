using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Net;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
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
        // make sure that we don't use too much memory for subscription batch
        private readonly Size _maximumAllowedMemory;
        private readonly SubscriptionConnectionsState _subscriptionConnectionsState;

        public SubscriptionDocumentsFetcher(DocumentDatabase db, int maxBatchSize, long subscriptionId, EndPoint remoteEndpoint, string collection,
            bool revisions,
            SubscriptionState subscription,
            SubscriptionPatchDocument patch,
            SubscriptionConnectionsState subscriptionConnectionsState)
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
            _maximumAllowedMemory = new Size((_db.Is32Bits ? 4 : 32) * Voron.Global.Constants.Size.Megabyte, SizeUnit.Bytes);
            _subscriptionConnectionsState = subscriptionConnectionsState;
        }

        public IEnumerable<(Document Doc, Exception Exception)> GetDataToSend(
            ClusterOperationContext clusterOperationContext,
            DocumentsOperationContext docsContext,
            IncludeDocumentsCommand includesCmd,
            long startEtag)
        {
            if (string.IsNullOrEmpty(_collection))
                throw new ArgumentException("The collection name must be specified");
            
            if (_revisions)
            {
                if (_db.DocumentsStorage.RevisionsStorage.Configuration == null ||
                    _db.DocumentsStorage.RevisionsStorage.GetRevisionsConfiguration(_collection).Disabled)
                    throw new SubscriptionInvalidStateException($"Cannot use a revisions subscription, database {_db.Name} does not have revisions configuration.");

                return GetRevisionsToSend(docsContext, includesCmd, _subscriptionConnectionsState.GetRevisionsFromResend(clusterOperationContext), startEtag);
            }

            return GetDocumentsToSend(docsContext, includesCmd, _subscriptionConnectionsState.GetDocumentsFromResend(clusterOperationContext), startEtag);
        }
        
        private IEnumerable<(Document Doc, Exception Exception)> GetDocumentsToSend(DocumentsOperationContext docsContext,
             IncludeDocumentsCommand includesCmd, IEnumerable<DocumentRecord> resendDocuments,
            long startEtag)
        {
            int numberOfDocs = 0;
            long size = 0;

            using (_db.Scripts.GetScriptRunner(_patch, true, out var run))
            {
                var docsByEtag = GetDocumentsByEtag(docsContext, startEtag);

                var resendDocs = GetDocumentsFromResend(docsContext, resendDocuments);

                var docs = MergeEnumerators(resendDocs, docsByEtag);

                foreach (var doc in docs)
                {
                    using (doc.Data)
                    {
                        size += doc.Data.Size;
                        if (ShouldSendDocument(_subscription, run, _patch, docsContext, doc, out BlittableJsonReaderObject transformResult, out var exception) == false)
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
                            if (includesCmd != null && run != null)
                                includesCmd.AddRange(run.Includes, doc.Id);

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
                                        Flags = doc.Flags,
                                        StorageId = doc.StorageId,
                                        NonPersistentFlags = doc.NonPersistentFlags,
                                        TransactionMarker = doc.TransactionMarker
                                        
                                    };

                                    yield return (projection, null);
                                }
                            }
                        }
                    }

                    if (++numberOfDocs >= _maxBatchSize)
                        yield break;

                    if (size + docsContext.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize.GetValue(SizeUnit.Bytes) >= _maximumAllowedMemory.GetValue(SizeUnit.Bytes))
                        yield break;
                }
            }
        }

        private IEnumerable<Document> GetDocumentsByEtag(DocumentsOperationContext docsContext, long startEtag)
        {
            var docs = _collection switch
            {
                Constants.Documents.Collections.AllDocumentsCollection =>
                    _db.DocumentsStorage.GetDocumentsFrom(docsContext, startEtag + 1, 0, long.MaxValue),
                _ =>
                    _db.DocumentsStorage.GetDocumentsFrom(
                        docsContext,
                        _collection,
                        startEtag + 1,
                        0,
                        long.MaxValue)
            };
            return docs;
        }

        private IEnumerable<Document> GetDocumentsFromResend(DocumentsOperationContext docsContext, IEnumerable<DocumentRecord> documents)
        {
            // resend docs are un-ordered
            foreach (var record in documents)
            {
                var current = _db.DocumentsStorage.GetDocumentOrTombstone(docsContext, record.DocumentId, throwOnConflict: false);
                if (HasDocBeenUpdatedBeyondGivenChangeVector(current, record.ChangeVector))
                {
                    Debug.Assert(current.Document != null, "Document does not exist");
                    yield return current.Document;
                }
            }
        }

        internal bool HasDocBeenUpdatedBeyondGivenChangeVector(DocumentsStorage.DocumentOrTombstone item, string currentChangeVector)
        {
            if (item.Document != null)
            {
                switch (_db.DocumentsStorage.GetConflictStatus(item.Document.ChangeVector, currentChangeVector))
                {
                    case ConflictStatus.Update:
                        // If document was updated, but the subscription went too far.
                        // We need to resend it
                        return _db.DocumentsStorage.GetConflictStatus(item.Document.ChangeVector, _subscriptionConnectionsState.LastChangeVectorSent) == ConflictStatus.AlreadyMerged;

                    case ConflictStatus.AlreadyMerged:
                        return true;

                    case ConflictStatus.Conflict:
                        return false;

                    default:
                        throw new InvalidEnumArgumentException();
                }
            }
            // TODO: we probably need to delete it from the resend table
            // we don't send tombstones
            return false;
        }


        private IEnumerable<(Document Previous, Document Current)> GetRevisionsEnumerator(IEnumerable<(Document previous, Document current)> enumerable) {
            foreach (var item in enumerable)
            {
                if (item.current.Flags.HasFlag(DocumentFlags.DeleteRevision))
                {
                    yield return (item.current, null);
                }
                else
                {
                    yield return item;
                }
            }
        }

        public class SubscriptionRevision : Document
        {
            public string Previous;
        }

        private IEnumerable<(Document Doc, Exception Exception)> GetRevisionsToSend(
            DocumentsOperationContext docsContext,
            IncludeDocumentsCommand includesCmd,
            IEnumerable<RevisionRecord> resendRevisions,
            long startEtag)
        {
            int numberOfDocs = 0;
            Size size = new Size(0, SizeUnit.Megabytes);

            using (_db.Scripts.GetScriptRunner(_patch, true, out var run))
            {
                var revisionsByEtag = GetRevisionsByEtag(docsContext, startEtag);
                
                var revisionsFromResend = GetRevisionsFromResend(docsContext, resendRevisions);

                var revisions = MergeEnumerators(revisionsFromResend, revisionsByEtag);

                foreach (var revisionTuple in GetRevisionsEnumerator(revisions))
                {
                    var item = (revisionTuple.Current ?? revisionTuple.Previous);
                    Debug.Assert(revisionTuple.Current != null);
                    Debug.Assert(item != null);
                    size.Add(item.Data.Size, SizeUnit.Bytes);
                    if (ShouldSendDocumentWithRevisions(_subscription, run, _patch, docsContext, item, revisionTuple, out var transformResult, out var exception) == false)
                    {
                        if (includesCmd != null && run != null)
                            includesCmd.AddRange(run.Includes, item.Id);

                        if (exception != null)
                        {
                            yield return (item, exception);
                        }
                        else
                        {
                            // make sure that if we read a lot of irrelevant documents, we send keep alive over the network
                            yield return (new SubscriptionRevision
                            {
                                Data = null,
                                ChangeVector = item.ChangeVector,
                                Etag = item.Etag,
                                LastModified = item.LastModified,                                
                                Flags = item.Flags,
                                StorageId = item.StorageId,
                                NonPersistentFlags = item.NonPersistentFlags,
                                TransactionMarker = item.TransactionMarker,
                                Previous = revisionTuple.Previous?.ChangeVector ?? string.Empty
                            }, null);
                        }
                    }
                    else
                    {
                        using (transformResult)
                        {
                            if (transformResult == null)
                            {
                                yield return (revisionTuple.Current, null);
                            }
                            else
                            {
                                var projection = new SubscriptionRevision
                                {
                                    Id = item.Id,
                                    Etag = item.Etag,
                                    Data = transformResult,
                                    LowerId = item.LowerId,
                                    ChangeVector = item.ChangeVector,
                                    LastModified = item.LastModified,
                                    Flags = item.Flags,
                                    StorageId = item.StorageId,
                                    NonPersistentFlags = item.NonPersistentFlags,
                                    TransactionMarker = item.TransactionMarker,
                                    Previous = revisionTuple.Previous?.ChangeVector ?? string.Empty
                                };

                                yield return (projection, null);
                            }
                        }
                    }
                    if (++numberOfDocs >= _maxBatchSize)
                        yield break;

                    if (size.GetValue(SizeUnit.Bytes) + docsContext.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize.GetValue(SizeUnit.Bytes) >= _maximumAllowedMemory.GetValue(SizeUnit.Bytes))
                        yield break;
                }
            }
        }

        private IEnumerable<(Document, Document)> GetRevisionsByEtag(DocumentsOperationContext docsContext, long startEtag)
        {
            return _collection switch
            {
                Constants.Documents.Collections.AllDocumentsCollection =>
                    _db.DocumentsStorage.RevisionsStorage.GetRevisionsFrom(docsContext, startEtag + 1, 0, long.MaxValue),
                _ =>
                    _db.DocumentsStorage.RevisionsStorage.GetRevisionsFrom(docsContext, new CollectionName(_collection), startEtag + 1, long.MaxValue)
            };
        }

        private bool _fetchingFromResend;

        private IEnumerable<T> MergeEnumerators<T>(IEnumerable<T> resendItems, IEnumerable<T> items)
        {
            _fetchingFromResend = false;
            foreach (var item in resendItems)
            {
                _fetchingFromResend = true;
                yield return item;
            }

            if (_fetchingFromResend)
            {
                // we don't mix documents from resend and regular, so we need to do another round when we are done with the resend
                _subscriptionConnectionsState.NotifyHasMoreDocs(); 
                yield break;
            }

            foreach (var item in items)
            {
                yield return item;
            }
        }

        private IEnumerable<(Document Previous, Document Current)> GetRevisionsFromResend(DocumentsOperationContext context, IEnumerable<RevisionRecord> resend)
        {
            foreach (var r in resend)
            {
                Console.WriteLine($"from resend {r.Previous} - {r.Current}");
                yield return (_db.DocumentsStorage.RevisionsStorage.GetRevision(context, r.Previous), _db.DocumentsStorage.RevisionsStorage.GetRevision(context, r.Current));
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

            if (_fetchingFromResend == false)
            {
                var conflictStatus = ChangeVectorUtils.GetConflictStatus(
                    remoteAsString: doc.ChangeVector,
                    localAsString: subscriptionState.ChangeVectorForNextBatchStartingPoint);

                if (conflictStatus == ConflictStatus.AlreadyMerged)
                    return false;
            }
            
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

            if (_fetchingFromResend == false)
            {
                var conflictStatus = ChangeVectorUtils.GetConflictStatus(
                    remoteAsString: item.ChangeVector,
                    localAsString: subscriptionState.ChangeVectorForNextBatchStartingPoint);

                if (conflictStatus == ConflictStatus.AlreadyMerged)
                    return false;
            }

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
                    json.Set(Constants.Documents.Metadata.Key, metadata, false);
                }

                metadata.Set(Constants.Documents.Metadata.Projection, JsBoolean.True, false);
            }
        }
    }
}
