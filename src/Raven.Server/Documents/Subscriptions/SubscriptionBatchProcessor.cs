using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Subscriptions
{
    public class CollectedDocument
    {
        public string Id;
        public string ChangeVector;
        public string PreviousChangeVector;
    }

    public abstract class SubscriptionBatchProcessor<T> : SubscriptionBatchProcessor
    {
        protected Logger Logger;

        protected SubscriptionBatchProcessor(DocumentDatabase database, SubscriptionConnectionsState subscriptionConnectionsState, string collection) : base(database, subscriptionConnectionsState, collection)
        {
            Logger = LoggingSource.Instance.GetLogger<SubscriptionBatchProcessor<T>>(Database.Name);
        }

        protected abstract bool ShouldSend(T item, out string reason, out Exception exception);

        protected abstract IEnumerable<T> FetchByEtag();

        protected abstract IEnumerable<T> FetchFromResend();

        private protected IEnumerable<T> GetEnumerator()
        {
            FetchingFrom = FetchingOrigin.None;
            foreach (var item in FetchFromResend())
            {
                FetchingFrom = FetchingOrigin.Resend;
                yield return item;
            }

            if (FetchingFrom == FetchingOrigin.Resend)
            {
                // we don't mix resend and regular, so we need to do another round when we are done with the resend
                SubscriptionConnectionsState.NotifyHasMoreDocs(); 
                yield break;
            }

            FetchingFrom = FetchingOrigin.Storage;
            foreach (var item in FetchByEtag())
            {
                yield return item;
            }
        }
    }

    public abstract class SubscriptionBatchProcessor : IDisposable
    {
        protected readonly DocumentDatabase Database;
        protected readonly SubscriptionConnectionsState SubscriptionConnectionsState;
        protected readonly string Collection;
        protected readonly long SubscriptionId;

        protected readonly Size MaximumAllowedMemory;

        public List<CollectedDocument> CollectedDocuments;

        public static SubscriptionBatchProcessor Create(DocumentDatabase database, SubscriptionConnectionsState subscriptionConnectionsState, SubscriptionConnection.ParsedSubscription subscription)
        {
            if (subscription.Revisions)
                return new RevisionSubscriptionBatchProcessor(database, subscriptionConnectionsState, subscription.Collection);

            return new DocumentSubscriptionBatchProcessor(database, subscriptionConnectionsState, subscription.Collection);
        }

        protected SubscriptionBatchProcessor(DocumentDatabase database, SubscriptionConnectionsState subscriptionConnectionsState, string collection)
        {
            Database = database;
            SubscriptionConnectionsState = subscriptionConnectionsState;
            SubscriptionId = subscriptionConnectionsState.SubscriptionId;
            Collection = collection;

            MaximumAllowedMemory = new Size((Database.Is32Bits ? 4 : 32) * Voron.Global.Constants.Size.Megabyte, SizeUnit.Bytes);
        }

        protected SubscriptionState Subscription;
        protected EndPoint RemoteEndpoint;
        protected SubscriptionWorkerOptions Options;
        
        protected int BatchSize => Options.MaxDocsPerBatch;

        public void SetConnectionInfo(SubscriptionWorkerOptions options, SubscriptionState subscription, EndPoint endpoint)
        {
            Options = options;
            Subscription = subscription;
            RemoteEndpoint = endpoint;
        }

        protected SubscriptionPatchDocument Patch;
        protected ScriptRunner.SingleRun Run;
        private ScriptRunner.ReturnRun? _returnRun;

        public void AddScript(SubscriptionPatchDocument patch)
        {
            Patch = patch;
        }

        protected void InitializeScript()
        {
            if (Patch == null)
                return;

            if (_returnRun != null)
                return; // already init

            _returnRun = Database.Scripts.GetScriptRunner(Patch, true, out Run);
        }

        protected HashSet<long> Active;
        protected ClusterOperationContext ClusterContext;
        protected DocumentsOperationContext DocsContext;
        protected IncludeDocumentsCommand IncludesCmd;
        protected long StartEtag;

        public virtual void Initialize(
            ClusterOperationContext clusterContext,
            DocumentsOperationContext docsContext,
            IncludeDocumentsCommand includesCmd)
        {
            ClusterContext = clusterContext;
            DocsContext = docsContext;
            IncludesCmd = includesCmd;

            Active = SubscriptionConnectionsState.GetConnections().Select(conn => conn.CurrentBatchId).ToHashSet();
            StartEtag = SubscriptionConnectionsState.GetLastEtagSent();

            CollectedDocuments ??= new List<CollectedDocument>();
            CollectedDocuments?.Clear();
        }

        public abstract IEnumerable<(Document Doc, Exception Exception)> Fetch();

        public abstract Task<long> Record(string lastChangeVectorSentInThisBatch);

        protected Task<long> RecordEmptyBatch(string lastChangeVectorSentInThisBatch)
        {
            return Database.SubscriptionStorage.RecordBatchDocuments(
                SubscriptionId,
                SubscriptionConnectionsState.SubscriptionName,
                new List<DocumentRecord>(),
                SubscriptionConnectionsState.PreviouslyRecordedChangeVector,
                lastChangeVectorSentInThisBatch);
        }

        protected enum FetchingOrigin
        {
            None,
            Resend,
            Storage
        }

        protected FetchingOrigin FetchingFrom;
      
        private protected class ProjectionMetadataModifier : JsBlittableBridge.IResultModifier
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

        public void Dispose()
        {
            ClusterContext = null;
            DocsContext = null;
            _returnRun?.Dispose();
        }
    }

    public class RevisionSubscriptionBatchProcessor : SubscriptionBatchProcessor<(Document Previous, Document Current)>
    {
        public RevisionSubscriptionBatchProcessor(DocumentDatabase database, SubscriptionConnectionsState subscriptionConnectionsState, string collection) : base(database, subscriptionConnectionsState, collection)
        {
        
        }

        protected override bool ShouldSend((Document Previous, Document Current) item, out string reason, out Exception exception)
        {
            exception = null;
            reason = null;

            if (FetchingFrom == FetchingOrigin.Resend)
            {
                var conflictStatus = ChangeVectorUtils.GetConflictStatus(
                    remoteAsString: item.Current.ChangeVector,
                    localAsString: Subscription.ChangeVectorForNextBatchStartingPoint);

                if (conflictStatus == ConflictStatus.AlreadyMerged)
                {
                    reason = $"{item.Current.Id} is already merged";
                    return false;
                }

                if (SubscriptionConnectionsState.IsRevisionInActiveBatch(ClusterContext, item.Current.ChangeVector, Active))
                {
                    reason = $"{item.Current.Id} is in active batch";
                    return false;
                }
            }

            item.Current.EnsureMetadata();
            item.Previous?.EnsureMetadata();

            var transformResult = DocsContext.ReadObject(new DynamicJsonValue
            {
                ["Current"] = item.Current.Data,
                ["Previous"] = item.Previous?.Data
            }, item.Current.Id);

            item.Current.Data = transformResult;

            if (Patch == null)
                return true;

            item.Current.ResetModifications();
            item.Previous?.ResetModifications();

            try
            {
                InitializeScript();
                var result = Patch.MatchCriteria(Run, DocsContext, transformResult, ProjectionMetadataModifier.Instance, ref transformResult);
                if (result == false)
                {
                    reason = $"{item.Current.Id} filtered by criteria";
                    return false;
                }

                item.Current.Data = transformResult;
                return true;
            }
            catch (Exception ex)
            {
                reason = $"Criteria script threw exception for subscription {SubscriptionId} connected to {RemoteEndpoint} for document id {item.Current.Id}";
                exception = ex;
                return false;
            }
        }

        protected override IEnumerable<(Document, Document)> FetchByEtag()
        {
            return Collection switch
            {
                Constants.Documents.Collections.AllDocumentsCollection =>
                    Database.DocumentsStorage.RevisionsStorage.GetRevisionsFrom(DocsContext, StartEtag + 1, 0, long.MaxValue),
                _ =>
                    Database.DocumentsStorage.RevisionsStorage.GetRevisionsFrom(DocsContext, new CollectionName(Collection), StartEtag + 1, long.MaxValue)
            };
        }

        protected override IEnumerable<(Document, Document)> FetchFromResend()
        {
            foreach (var r in SubscriptionConnectionsState.GetRevisionsFromResend(ClusterContext, Active))
            {
                yield return (
                    Database.DocumentsStorage.RevisionsStorage.GetRevision(DocsContext, r.Previous), 
                    Database.DocumentsStorage.RevisionsStorage.GetRevision(DocsContext, r.Current)
                    );
            }
        }

        public override IEnumerable<(Document Doc, Exception Exception)> Fetch()
        {
            if (Database.DocumentsStorage.RevisionsStorage.Configuration == null ||
                Database.DocumentsStorage.RevisionsStorage.GetRevisionsConfiguration(Collection).Disabled)
                throw new SubscriptionInvalidStateException($"Cannot use a revisions subscription, database {Database.Name} does not have revisions configuration.");

            Size size = default;
            var numberOfDocs = 0;

            foreach (var item in GetEnumerator())
            {
                Debug.Assert(item.Current != null);
                if(item.Current == null)
                    continue; // this shouldn't happened, but in release let's keep running

                size += new Size(item.Current.Data?.Size ?? 0, SizeUnit.Bytes);

                using (item.Current)
                using (item.Previous)
                {
                    if (ShouldSend(item, out var reason, out var exception))
                    {
                        CollectedDocuments?.Add(new CollectedDocument
                        {
                            Id = item.Current.Id,
                            ChangeVector = item.Current.ChangeVector,
                            PreviousChangeVector = item.Previous?.ChangeVector
                        });

                        yield return (item.Current, exception);
                    }
                    else
                    {
                        if (Logger.IsInfoEnabled)
                        {
                            Logger.Info(reason, exception);
                        }

                        if (IncludesCmd != null && Run != null)
                            IncludesCmd.AddRange(Run.Includes, item.Current.Id);

                        if (exception != null)
                        {
                            yield return (item.Current, exception);
                        }
                        else
                        {
                            item.Current.Data = null;
                            yield return (item.Current, null);
                        }
                    }

                    item.Current.Data?.Dispose(); 
                    item.Current.Data = null;

                    if (size + DocsContext.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize >= MaximumAllowedMemory)
                        yield break;

                    if (++numberOfDocs >= BatchSize)
                        yield break;
                }
            }
        }

        public override Task<long> Record(string lastChangeVectorSentInThisBatch)
        {
            if (CollectedDocuments == null || CollectedDocuments.Count == 0)
                return RecordEmptyBatch(lastChangeVectorSentInThisBatch);

            return Database.SubscriptionStorage.RecordBatchRevisions(
                SubscriptionId,
                SubscriptionConnectionsState.SubscriptionName,
                CollectedDocuments.Select(x=> new RevisionRecord{Current= x.ChangeVector, Previous= x.PreviousChangeVector}).ToList(),
                SubscriptionConnectionsState.PreviouslyRecordedChangeVector,
                lastChangeVectorSentInThisBatch);
        }
    }

    public class DocumentSubscriptionBatchProcessor : SubscriptionBatchProcessor<Document>
    {
        
        public DocumentSubscriptionBatchProcessor(DocumentDatabase database, SubscriptionConnectionsState subscriptionConnectionsState, string collection) : 
            base(database, subscriptionConnectionsState, collection)
        {
        }

        public override IEnumerable<(Document Doc, Exception Exception)> Fetch()
        {
            Size size = default;
            var numberOfDocs = 0;

            foreach (var item in GetEnumerator())
            {
                using (item.Data)
                {
                    size += new Size(item.Data?.Size ?? 0, SizeUnit.Bytes);
                    if (ShouldSend(item, out var reason, out var exception))
                    {
                        if (IncludesCmd != null && Run != null)
                            IncludesCmd.AddRange(Run.Includes, item.Id);

                        CollectedDocuments?.Add(new CollectedDocument
                        {
                            Id = item.Id,
                            ChangeVector = item.ChangeVector,
                        });

                        yield return (item, null);
                    }
                    else
                    {
                        if (Logger.IsInfoEnabled)
                        {
                            Logger.Info(reason, exception);
                        }

                        item.Data = null;
                        yield return (item, exception);
                    }

                    item.Data?.Dispose();
                    item.Data = null;

                    if (size + DocsContext.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize >= MaximumAllowedMemory)
                        yield break;

                    if (++numberOfDocs >= BatchSize)
                        yield break;
                }
            }
        }

        protected override bool ShouldSend(Document item, out string reason, out Exception exception)
        {
            BlittableJsonReaderObject transformResult = null;
            exception = null;
            reason = null;

            if (FetchingFrom == FetchingOrigin.Storage)
            {
                var conflictStatus = ChangeVectorUtils.GetConflictStatus(
                    remoteAsString: item.ChangeVector,
                    localAsString: Subscription.ChangeVectorForNextBatchStartingPoint);

                if (conflictStatus == ConflictStatus.AlreadyMerged)
                {
                    reason = $"{item.Id} is already merged";
                    return false;
                }

                if (SubscriptionConnectionsState.IsDocumentInActiveBatch(ClusterContext, item.Id, Active))
                {
                    reason = $"{item.Id} exists in an active batch";
                    return false;
                }
            }

            if (Patch == null)
                return true;

            try
            {
                InitializeScript();
                var result = Patch.MatchCriteria(Run, DocsContext, item, ProjectionMetadataModifier.Instance, ref transformResult);

                if (result == false)
                {
                    reason = $"{item.Id} filtered out by criteria";
                    return false;
                }
                
                item.Data = transformResult;
                return true;
            }
            catch (Exception ex)
            {
                exception = ex;
                reason = $"Criteria script threw exception for subscription {SubscriptionId} connected to {RemoteEndpoint} for document id {item.Id}";
                return false;
            }
        }

        public override Task<long> Record(string lastChangeVectorSentInThisBatch)
        {
            if (CollectedDocuments == null || CollectedDocuments.Count == 0)
                return RecordEmptyBatch(lastChangeVectorSentInThisBatch);

            return Database.SubscriptionStorage.RecordBatchDocuments(
                SubscriptionId,
                SubscriptionConnectionsState.SubscriptionName,
                CollectedDocuments.Select(x=> new DocumentRecord{DocumentId = x.Id, ChangeVector = x.ChangeVector}).ToList(),
                SubscriptionConnectionsState.PreviouslyRecordedChangeVector,
                lastChangeVectorSentInThisBatch);
        }

        protected override IEnumerable<Document> FetchByEtag()
        {
            return Collection switch
            {
                Constants.Documents.Collections.AllDocumentsCollection =>
                    Database.DocumentsStorage.GetDocumentsFrom(DocsContext, StartEtag + 1, 0, long.MaxValue),
                _ =>
                    Database.DocumentsStorage.GetDocumentsFrom(
                        DocsContext,
                        Collection,
                        StartEtag + 1,
                        0,
                        long.MaxValue)
            };
        }

        protected override IEnumerable<Document> FetchFromResend()
        {
            foreach (var record in SubscriptionConnectionsState.GetDocumentsFromResend(ClusterContext, Active))
            {
                var current = Database.DocumentsStorage.GetDocumentOrTombstone(DocsContext, record.DocumentId, throwOnConflict: false);
                if (ShouldFetchFromResend(current, record.ChangeVector))
                {
                    Debug.Assert(current.Document != null, "Document does not exist");
                    yield return current.Document;
                }
                // TODO stav: consider what happened to the heartbeat if we skip here a lot
            }
        }

        private bool ShouldFetchFromResend(DocumentsStorage.DocumentOrTombstone item, string currentChangeVector)
        {
            if (item.Document != null)
            {
                switch (Database.DocumentsStorage.GetConflictStatus(item.Document.ChangeVector, currentChangeVector))
                {
                    case ConflictStatus.Update:
                        // If document was updated, but the subscription went too far.
                        // We need to resend it
                        return Database.DocumentsStorage.GetConflictStatus(item.Document.ChangeVector, SubscriptionConnectionsState.LastChangeVectorSent) == ConflictStatus.AlreadyMerged;

                    case ConflictStatus.AlreadyMerged:
                        return true;

                    case ConflictStatus.Conflict:
                        return false;

                    default:
                        throw new InvalidEnumArgumentException();
                }
            }
            // TODO stav: we probably need to delete it from the resend table
            // we don't send tombstones
            return false;
        }
    }

    public class DummyDocumentSubscriptionBatchProcessor : DocumentSubscriptionBatchProcessor
    {
        public DummyDocumentSubscriptionBatchProcessor(DocumentDatabase database, SubscriptionState state, string collection) : 
            base(database, SubscriptionConnectionsState.CreateDummyState(database.DocumentsStorage, state), collection)
        {
        }

        public override void Initialize(ClusterOperationContext clusterContext, DocumentsOperationContext docsContext, IncludeDocumentsCommand includesCmd)
        {
            ClusterContext = clusterContext;
            DocsContext = docsContext;
            IncludesCmd = includesCmd;

            Active = new HashSet<long>();
            StartEtag = 0;
        }

        public void SetStartEtag(long etag)
        {
            StartEtag = etag;
        }

        public override Task<long> Record(string lastChangeVectorSentInThisBatch)
        {
            return Task.FromResult(-1L);
        }
    }
}
