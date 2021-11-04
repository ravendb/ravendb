using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Logging;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor
{
    public abstract class SubscriptionProcessor<T> : SubscriptionProcessor
    {
        protected Logger Logger;
        protected SubscriptionFetcher<T> Fetcher;

        protected SubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection) :
            base(server, database, connection)
        {
            Logger = LoggingSource.Instance.GetLogger<SubscriptionProcessor<T>>(Database.Name);
        }

        protected abstract SubscriptionFetcher<T> CreateFetcher();

        public override void InitializeForNewBatch(ClusterOperationContext clusterContext, DocumentsOperationContext docsContext, IncludeDocumentsCommand includesCmd)
        {
            base.InitializeForNewBatch(clusterContext, docsContext, includesCmd);

            Fetcher = CreateFetcher();
            Fetcher.Initialize(clusterContext, docsContext, Active);
        }

        protected abstract bool ShouldSend(T item, out string reason, out Exception exception, out Document result);

        protected (Document Doc, Exception Exception) GetBatchItem(T item)
        {
            if (ShouldSend(item, out var reason, out var exception, out var result))
            {
                if (IncludesCmd != null && Run != null)
                    IncludesCmd.AddRange(Run.Includes, result.Id);

                if (result.Data != null)
                    Fetcher.MarkDocumentSent();

                return (result, null);
            }

            if (Logger.IsInfoEnabled) 
                Logger.Info(reason, exception);

            if (exception != null)
            {
                if (result.Data != null)
                    Fetcher.MarkDocumentSent();

                return (result, exception);
            }

            result.Data = null;
            return (result, null);
        }
    }

    public abstract class SubscriptionProcessor : IDisposable
    {
        protected readonly ServerStore Server;
        protected readonly DocumentDatabase Database;
        protected readonly SubscriptionConnection Connection;
        
        protected EndPoint RemoteEndpoint;
        protected readonly Size MaximumAllowedMemory;
        protected SubscriptionState SubscriptionState;
        protected SubscriptionWorkerOptions Options;
        protected string Collection;
        protected SubscriptionConnectionsState SubscriptionConnectionsState;

        protected int BatchSize => Options.MaxDocsPerBatch;

        public static SubscriptionProcessor Create(SubscriptionConnection connection)
        {
            var database = connection.TcpConnection.DocumentDatabase;
            var server = database.ServerStore;
            if (connection.Subscription.Revisions)
            {
                return new RevisionsSubscriptionProcessor(server, database, connection);
            }

            return new DocumentsSubscriptionProcessor(server, database, connection);
        }

        protected SubscriptionProcessor(ServerStore server, DocumentDatabase database, SubscriptionConnection connection)
        {
            Server = server;
            Database = database;
            Connection = connection;

            MaximumAllowedMemory = new Size((Database.Is32Bits ? 4 : 32) * Voron.Global.Constants.Size.Megabyte, SizeUnit.Bytes);
        }

        public virtual void InitializeProcessor()
        {
            SubscriptionConnectionsState = Connection.SubscriptionConnectionsState;
            Collection = Connection.Subscription.Collection;
            Options = Connection.Options;
            SubscriptionState = Connection.SubscriptionState;
            RemoteEndpoint = Connection.TcpConnection.TcpClient.Client.RemoteEndPoint;

            Active = SubscriptionConnectionsState.GetActiveBatches();
        }

        public abstract IEnumerable<(Document Doc, Exception Exception)> GetBatch();

        public abstract Task<long> RecordBatch(string lastChangeVectorSentInThisBatch);

        public abstract Task AcknowledgeBatch(long batchId);

        public abstract long GetLastItemEtag(DocumentsOperationContext context, string collection);

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
        protected DocumentsOperationContext DocsContext;
        protected ClusterOperationContext ClusterContext;
        protected IncludeDocumentsCommand IncludesCmd;

        public virtual void InitializeForNewBatch(
            ClusterOperationContext clusterContext,
            DocumentsOperationContext docsContext,
            IncludeDocumentsCommand includesCmd)
        {
            ClusterContext = clusterContext;
            DocsContext = docsContext;
            IncludesCmd = includesCmd;

            InitializeProcessor();
        }

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
            _returnRun?.Dispose();
        }
    }
}
