// -----------------------------------------------------------------------
//  <copyright file="ShardedSubscriptionConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Util;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Processor;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Utils;
using static Raven.Server.Documents.Subscriptions.ISubscriptionConnection;

namespace Raven.Server.Documents.Sharding.Subscriptions
{
    public sealed class OrchestratedSubscriptionConnection : SubscriptionConnectionBase<OrchestratorIncludesCommandImpl>
    {
        private SubscriptionConnectionsStateOrchestrator _state;
        private readonly ShardedDatabaseContext _databaseContext;
        private OrchestratedSubscriptionProcessor _processor;
        private readonly IDisposable _tokenRegisterDisposable;
        private readonly HashSet<string> _dbIdsToRemove;

        public OrchestratedSubscriptionConnection(ServerStore serverStore, ShardedDatabaseContext.ShardedSubscriptionsStorage subscriptions,
            TcpConnectionOptions tcpConnection, IDisposable tcpConnectionDisposable,
            JsonOperationContext.MemoryBuffer buffer)
            : base(subscriptions, tcpConnection, serverStore, buffer, tcpConnectionDisposable, tcpConnection.DatabaseContext.DatabaseName,
                tcpConnection.DatabaseContext.DatabaseShutdown)
        {
            _databaseContext = tcpConnection.DatabaseContext;
            _tokenRegisterDisposable = CancellationTokenSource.Token.Register(() => _processor?.CurrentBatch?.SetCancel());
            _dbIdsToRemove = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { _databaseContext.DatabaseRecord.Sharding.DatabaseId };
        }

        public SubscriptionConnectionsStateOrchestrator GetOrchestratedSubscriptionConnectionState()
        {
            var subscriptions = _databaseContext.SubscriptionsStorage.Subscriptions;
            return _state = subscriptions.GetOrAdd(SubscriptionId, subId => new SubscriptionConnectionsStateOrchestrator(ServerStore, _databaseContext, subId));
        }

        public override void FinishProcessing()
        {
            AddToStatusDescription(CreateStatusMessage(ConnectionStatus.Info, "Finished processing sharded subscription."));

            if (_logger.IsInfoEnabled)
                _logger.Info($"Finished processing sharded subscription '{SubscriptionId}' / from client '{ClientUri}'.");
        }

        protected override StatusMessageDetails GetStatusMessageDetails()
        {
            return new StatusMessageDetails
            {
                DatabaseName = $"for sharded database '{DatabaseName}' on '{ServerStore.NodeTag}'",
                ClientType = $"'client worker' with IP '{ClientUri}'",
                SubscriptionType = $"sharded subscription '{_options?.SubscriptionName}', id '{SubscriptionId}'"
            };
        }

        protected override async Task OnClientAckAsync(string clientReplyChangeVector)
        {
            var orchestratorCv = ChangeVectorUtils.MergeVectors(_state.LastChangeVectorSent, LastSentChangeVectorInThisConnection);
            await NotifyShardAboutBatchCompletionAsync(orchestratorCv);

            _state.LastChangeVectorSent = orchestratorCv;
            
            await SendConfirmAsync(_databaseContext.Time.GetUtcNow());
        }

        protected override void GatherIncludesForDocument(OrchestratorIncludesCommandImpl includeDocuments, Document document)
        {
            // no op
        }

        public override SubscriptionConnectionInfo CreateConnectionInfo()
        {
            return new SubscriptionConnectionInfo()
            {
                ClientUri = ClientUri,
                Query = _state.Query,
                LatestChangeVector = _state.LastChangeVectorSent,
                ConnectionException = ConnectionException,
                RecentSubscriptionStatuses = RecentSubscriptionStatuses.ToList(),
                Date = SystemTime.UtcNow,
                Strategy = Strategy,
                TcpConnectionStats = TcpConnection.GetConnectionStats(),
                LastConnectionStats = Stats.LastConnectionStats,
                LastBatchesStats = Stats.GetBatchesPerformanceStats,
                ArchivedDataProcessingBehavior = SubscriptionState.ArchivedDataProcessingBehavior
            };
        }

        protected override async Task<bool> WaitForChangedDocsAsync(AbstractSubscriptionConnectionsState state)
        {
            // nothing was sent to the client, but we need to let the shard know he can continue
            await NotifyShardAboutBatchCompletionAsync(orchestratorCv: null);

            return await base.WaitForChangedDocsAsync(state);
        }

        private async Task NotifyShardAboutBatchCompletionAsync(string orchestratorCv)
        {
            var batch = _processor.CurrentBatch;

            if (batch != null)
            {
                // let sharded subscription worker know that we sent the batch to the client and received an ack request from it
                batch.SendBatchToClientTcs.TrySetResult();
                batch.LastSentChangeVectorInBatch = orchestratorCv; //pass the orchestrator merged change vector to shard, so it will ack it

                // wait for sharded subscription worker to send ack to the shard subscription connection
                // and receive the confirm from the shard subscription connection
                await batch.ConfirmFromShardSubscriptionConnectionTcs.Task;

                _processor.CurrentBatch = null;
            }
        }

        public override Task SendNoopAckAsync(bool force = false) => Task.CompletedTask;

        protected override bool FoundAboutMoreDocs()
        {
            if (_state.Batches.Count > 0)
                return true;

            AssertCloseWhenNoDocsLeft();
            return false;
        }

        protected override SubscriptionConnectionInUse MarkInUse()
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-19085 Do we need something like this in the database context?");

            return null;
        }

        protected override void AfterProcessorCreation()
        {
            _processor = Processor as OrchestratedSubscriptionProcessor;
        }

        protected override void RaiseNotificationForBatchEnd(string name, SubscriptionBatchStatsAggregator last)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-19085 Need to implement events + ws for this");
        }

        protected override Task<SubscriptionBatchStatus> TryRecordBatchAndUpdateStatusAsync(IChangeVectorOperationContext context, SubscriptionBatchResult result)
        {
            var vector = context.GetChangeVector(result.LastChangeVectorSentInThisBatch);
            vector.TryRemoveIds(_dbIdsToRemove, context, out vector);

            LastSentChangeVectorInThisConnection = vector.Order;

            if (result.CurrentBatch.Count == 0)
                return Task.FromResult(SubscriptionBatchStatus.EmptyBatch);

            return Task.FromResult(SubscriptionBatchStatus.DocumentsSent);
        }

        protected override void AssertSupportedFeatures()
        {
            base.AssertSupportedFeatures();

            if (_options.Strategy == SubscriptionOpeningStrategy.Concurrent)
                throw new NotSupportedInShardingException("Concurrent subscriptions are not supported in sharding.");
        }

        protected override void AssertCloseWhenNoDocsLeft()
        {
            if (_state.SubscriptionClosedDueNoDocs)
            {
                base.AssertCloseWhenNoDocsLeft();
            }
        }

        public override ISubscriptionProcessor<OrchestratorIncludesCommandImpl> CreateProcessor(SubscriptionConnectionBase<OrchestratorIncludesCommandImpl> connection)
        {
            if (connection is OrchestratedSubscriptionConnection orchestratedSubscription)
                return new OrchestratedSubscriptionProcessor(connection.TcpConnection.DatabaseContext.ServerStore, connection.TcpConnection.DatabaseContext, orchestratedSubscription);

            throw new InvalidOperationException($"Expected to create a processor for '{nameof(OrchestratedSubscriptionConnection)}', but got: '{connection.GetType().Name}'.");
        }

        protected override void OnError(Exception e) => _processor?.CurrentBatch?.SetException(e);

        protected override void DisposeInternal()
        {
            _tokenRegisterDisposable?.Dispose();

            base.DisposeInternal();
        }
    }
}
