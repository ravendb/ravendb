using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Sparrow.Server;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Subscriptions;

public class SubscriptionConnectionsStateOrchestrator : SubscriptionConnectionsStateBase<OrchestratedSubscriptionConnection>
{
    private readonly ShardedDatabaseContext _databaseContext;
    public AsyncManualResetEvent HasNewDocuments;

    public SubscriptionConnectionsStateOrchestrator(ServerStore server, ShardedDatabaseContext databaseContext, long subscriptionId) : base(server, databaseContext.DatabaseName, subscriptionId)
    {
        _databaseContext = databaseContext;
        CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(databaseContext.DatabaseShutdown);
        HasNewDocuments = new AsyncManualResetEvent(CancellationTokenSource.Token);
    }

    public override async Task UpdateClientConnectionTime()
    {
        var command = GetUpdateSubscriptionClientConnectionTime();
        var (etag, _) = await _server.SendToLeaderAsync(command);
        await _server.Cluster.WaitForIndexNotification(etag);
        // await WaitForIndexNotificationAsync(etag);
    }

    protected override UpdateSubscriptionClientConnectionTime GetUpdateSubscriptionClientConnectionTime()
    {
        var cmd = base.GetUpdateSubscriptionClientConnectionTime();
        cmd.DatabaseName = _databaseContext.DatabaseName;
        return cmd;
    }

    public override Task WaitForIndexNotificationAsync(long index) => _databaseContext.Cluster.WaitForExecutionOnShardsAsync(index).AsTask();

    public override void DropSubscription(SubscriptionException e)
    {
        foreach (var subscriptionConnection in GetConnections())
        {
            DropSingleConnection(subscriptionConnection, e);
        }
    }

    public override void NotifyHasMoreDocs() => HasNewDocuments.SetAndResetAtomically();
}
