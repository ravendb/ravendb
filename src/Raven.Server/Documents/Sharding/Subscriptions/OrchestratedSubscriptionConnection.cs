// -----------------------------------------------------------------------
//  <copyright file="ShardedSubscriptionConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// ----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Stats;
using Raven.Server.Documents.Subscriptions.SubscriptionProcessor;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Subscriptions
{
    public class OrchestratedSubscriptionConnection : SubscriptionConnectionBase
    {
        private SubscriptionConnectionsStateOrchestrator _state;
        private readonly ShardedDatabaseContext _databaseContext;
        private OrchestratedSubscriptionProcessor _processor;
        private IDisposable _tokenRegisterDisposable;

        public OrchestratedSubscriptionConnection(ServerStore serverStore, TcpConnectionOptions tcpConnection, IDisposable tcpConnectionDisposable,
            JsonOperationContext.MemoryBuffer buffer)
            : base(tcpConnection, serverStore, buffer, tcpConnectionDisposable, tcpConnection.DatabaseContext.DatabaseName,
                tcpConnection.DatabaseContext.DatabaseShutdown)
        {
            _databaseContext = tcpConnection.DatabaseContext;
            _tokenRegisterDisposable = CancellationTokenSource.Token.Register(() => _processor?.CurrentBatch?.SetCancel());
        }

        public SubscriptionConnectionsStateOrchestrator GetOrchestratedSubscriptionConnectionState()
        {
            var subscriptions = _databaseContext.Subscriptions.SubscriptionsConnectionsState;
            return _state = subscriptions.GetOrAdd(SubscriptionId, subId => new SubscriptionConnectionsStateOrchestrator(_serverStore, _databaseContext, subId));
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
                DatabaseName = $"for sharded database '{DatabaseName}' on '{_serverStore.NodeTag}'",
                ClientType = $"'client worker' with IP '{ClientUri}'",
                SubscriptionType = $"sharded subscription '{_options?.SubscriptionName}', id '{SubscriptionId}'"
            };
        }

        protected override async Task OnClientAckAsync()
        {
            var batch = _processor.CurrentBatch;

            // let sharded subscription worker know that we sent the batch to the client and received an ack request from it
            batch.SendBatchToClientTcs.TrySetResult();

            // wait for sharded subscription worker to send ack to the shard subscription connection
            // and receive the confirm from the shard subscription connection
            await batch.ConfirmFromShardSubscriptionConnectionTcs.Task;

            await SendConfirmAsync(_databaseContext.Time.GetUtcNow());

            _processor.CurrentBatch = null;
        }

        public override Task SendNoopAckAsync() => Task.CompletedTask;

        protected override bool FoundAboutMoreDocs() => _state.Batches.Count > 0;

        public override IDisposable MarkInUse()
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

        protected override string SetLastChangeVectorInThisBatch(IChangeVectorOperationContext context, string currentLast, Document sentDocument)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-19085 All tests pass but wonder if that is correct?");
            return sentDocument.ChangeVector;
        }

        protected override Task UpdateStateAfterBatchSentAsync(string lastChangeVectorSentInThisBatch)
        {
            _processor.CurrentBatch.LastSentChangeVectorInBatch = lastChangeVectorSentInThisBatch;
            return Task.CompletedTask;
        }

        protected override void AssertSupportedFeatures()
        {
            base.AssertSupportedFeatures();

            if (_options.Strategy == SubscriptionOpeningStrategy.Concurrent)
                throw new NotSupportedInShardingException("Concurrent subscriptions are not supported in sharding.");
        }

        protected override void OnError(Exception e) => _processor?.CurrentBatch?.SetException(e);

        public override void Dispose()
        {
            if (_isDisposed)
                return;

            _isDisposed = true;

            _tokenRegisterDisposable?.Dispose();
            base.Dispose();
        }
    }
}
