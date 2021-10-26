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
    public abstract class SubscriptionFetcher<T, TProcessor> : SubscriptionFetcher where TProcessor : SubscriptionProcessor
    {
        protected Logger Logger;

        protected SubscriptionFetcher(DocumentDatabase database, SubscriptionConnectionsState subscriptionConnectionsState, string collection) : base(database, subscriptionConnectionsState, collection)
        {
            Logger = LoggingSource.Instance.GetLogger<SubscriptionFetcher<T, TProcessor>>(Database.Name);
        }

        protected abstract bool ShouldSend(T item, out string reason, out Exception exception, out Document result);

        protected TProcessor Processor;

        public void SetProcessor(TProcessor processor) => Processor = processor;

        protected (Document Doc, Exception Exception) GetBatchItem(T item)
        {
            if (ShouldSend(item, out var reason, out var exception, out var result))
            {
                if (IncludesCmd != null && Run != null)
                    IncludesCmd.AddRange(Run.Includes, result.Id);

                return (result, null);
            }

            if (Logger.IsInfoEnabled) 
                Logger.Info(reason, exception);

            if (exception != null)
                return (result, exception);

            result.Data = null;
            return (result, null);
        }

        protected bool SuccessfulSend(Document doc, Exception exception) => doc.Data != null && exception == null;

        protected abstract IEnumerable<T> FetchByEtag();

        protected abstract IEnumerable<T> FetchFromResend();

        private protected IEnumerable<T> GetEnumerator()
        {
            FetchingFrom = FetchingOrigin.Resend;
            foreach (var item in FetchFromResend())
            {
                yield return item;
            }

            if (DocSent)
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

    public abstract class SubscriptionFetcher : IDisposable
    {
        protected readonly DocumentDatabase Database;
        protected readonly SubscriptionConnectionsState SubscriptionConnectionsState;
        protected readonly string Collection;
        protected readonly long SubscriptionId;

        protected readonly Size MaximumAllowedMemory;

        protected SubscriptionFetcher(DocumentDatabase database, SubscriptionConnectionsState subscriptionConnectionsState, string collection)
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

            DocSent = false;
        }
        
        public abstract IEnumerable<(Document Doc, Exception Exception)> Fetch();

        protected enum FetchingOrigin
        {
            None,
            Resend,
            Storage
        }

        protected bool DocSent;

        public void MarkDocumentSent()
        {
            DocSent = true;
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

    public class RevisionSubscriptionFetcher : SubscriptionFetcher<(Document Previous, Document Current), RevisionsSubscriptionProcessor>
    {
        public RevisionSubscriptionFetcher(DocumentDatabase database, SubscriptionConnectionsState subscriptionConnectionsState, string collection) : base(database, subscriptionConnectionsState, collection)
        {
        
        }

        protected override bool ShouldSend((Document Previous, Document Current) item, out string reason, out Exception exception, out Document result)
        {
            exception = null;
            reason = null;
            result = item.Current;

            if (FetchingFrom == FetchingOrigin.Storage)
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
                ["Current"] = item.Current.Flags.Contain(DocumentFlags.DeleteRevision) ? null : item.Current.Data,
                ["Previous"] = item.Previous?.Data
            }, item.Current.Id);

            result.Data = transformResult;

            if (Patch == null)
                return true;

            item.Current.ResetModifications();
            item.Previous?.ResetModifications();

            try
            {
                InitializeScript();
                var match = Patch.MatchCriteria(Run, DocsContext, transformResult, ProjectionMetadataModifier.Instance, ref result.Data);
                if (match == false)
                {
                    result.Data = null;
                    reason = $"{item.Current.Id} filtered by criteria";
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                reason = $"Criteria script threw exception for subscription {SubscriptionId} connected to {RemoteEndpoint} for document id {item.Current.Id}";
                exception = ex;
                return false;
            }
        }

        protected override IEnumerable<(Document Previous, Document Current)> FetchByEtag()
        {
            return Collection switch
            {
                Constants.Documents.Collections.AllDocumentsCollection =>
                    Database.DocumentsStorage.RevisionsStorage.GetRevisionsFrom(DocsContext, StartEtag + 1, 0, long.MaxValue),
                _ =>
                    Database.DocumentsStorage.RevisionsStorage.GetRevisionsFrom(DocsContext, new CollectionName(Collection), StartEtag + 1, long.MaxValue)
            };
        }

        protected override IEnumerable<(Document Previous, Document Current)> FetchFromResend()
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
            
            Processor.BatchItems.Clear();

            foreach (var item in GetEnumerator())
            {
                Debug.Assert(item.Current != null);
                if (item.Current == null)
                    continue; // this shouldn't happened, but in release let's keep running

                size += new Size(item.Current.Data?.Size ?? 0, SizeUnit.Bytes);

                using (item.Current)
                using (item.Previous)
                {
                    var result = GetBatchItem(item);
                    using (result.Doc)
                    {
                        if (SuccessfulSend(result.Doc,result.Exception))
                        {
                            Processor.BatchItems.Add(new RevisionRecord
                            {
                                Current = item.Current.ChangeVector,
                                Previous = item.Previous?.ChangeVector
                            });
                        }

                        yield return result;
                    }

                    if (size + DocsContext.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize >= MaximumAllowedMemory)
                        yield break;

                    if (++numberOfDocs >= BatchSize)
                        yield break;
                }
            }
        }
    }

    public class DocumentSubscriptionFetcher : SubscriptionFetcher<Document, DocumentsSubscriptionProcessor>
    {
        public DocumentSubscriptionFetcher(DocumentDatabase database, SubscriptionConnectionsState subscriptionConnectionsState, string collection) : 
            base(database, subscriptionConnectionsState, collection)
        {
        }

        public override IEnumerable<(Document Doc, Exception Exception)> Fetch()
        {
            Size size = default;
            var numberOfDocs = 0;

            Processor.ItemsToRemoveFromResend.Clear();
            Processor.BatchItems.Clear();

            foreach (var item in GetEnumerator())
            {
                using (item)
                {
                    size += new Size(item.Data?.Size ?? 0, SizeUnit.Bytes);

                    var result = GetBatchItem(item);
                    using (result.Doc)
                    {
                        if (SuccessfulSend(result.Doc,result.Exception))
                        {
                            Processor.AddItem(new DocumentRecord
                            {
                                DocumentId = result.Doc.Id,
                                ChangeVector = result.Doc.ChangeVector,
                            });
                        }

                        yield return result;
                    }

                    if (size + DocsContext.Transaction.InnerTransaction.LowLevelTransaction.AdditionalMemoryUsageSize >= MaximumAllowedMemory)
                        yield break;

                    if (++numberOfDocs >= BatchSize)
                        yield break;
                }
            }
        }

        protected override bool ShouldSend(Document item, out string reason, out Exception exception, out Document result)
        {
            exception = null;
            reason = null;
            result = item;

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

            if (FetchingFrom == FetchingOrigin.Resend)
            {
                var current = Database.DocumentsStorage.GetDocumentOrTombstone(DocsContext, item.Id, throwOnConflict: false);
                if (ShouldFetchFromResend(item.Id, current, item.ChangeVector) == false)
                {
                    item.ChangeVector = string.Empty;
                    reason = $"Skip {item.Id} from resend";
                    return false;
                }

                Debug.Assert(current.Document != null, "Document does not exist");
                result.Data = current.Document.Data;
                result.Etag = current.Document.Etag;
                result.ChangeVector = current.Document.ChangeVector;
            }

            if (Patch == null)
                return true;

            try
            {
                InitializeScript();
                var match = Patch.MatchCriteria(Run, DocsContext, item, ProjectionMetadataModifier.Instance, ref result.Data);

                if (match == false)
                {
                    result.Data = null;
                    reason = $"{item.Id} filtered out by criteria";
                    return false;
                }
                
                return true;
            }
            catch (Exception ex)
            {
                exception = ex;
                reason = $"Criteria script threw exception for subscription {SubscriptionId} connected to {RemoteEndpoint} for document id {item.Id}";
                return false;
            }
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
                yield return new Document
                {
                    Id = DocsContext.GetLazyString(record.DocumentId), 
                    ChangeVector = record.ChangeVector
                };
            }
        }

        private bool ShouldFetchFromResend(string id, DocumentsStorage.DocumentOrTombstone item, string currentChangeVector)
        {
            if (item.Document == null)
            {
                // the document was delete while it was processed by the client
                Processor.ItemsToRemoveFromResend.Add(id);
                return false;
            } 

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
    }

    public class DummyDocumentSubscriptionFetcher : DocumentSubscriptionFetcher
    {
        public DummyDocumentSubscriptionFetcher(DocumentDatabase database, SubscriptionState state, string collection) : 
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
    }
}
