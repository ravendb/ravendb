using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor;

public abstract class SubscriptionProcessor : IDisposable
{
    protected readonly ServerStore Server;
    protected readonly SubscriptionConnectionBase Connection;
        
    protected EndPoint RemoteEndpoint;
    protected SubscriptionState SubscriptionState;
    protected SubscriptionWorkerOptions Options;
    protected string Collection;

    protected int BatchSize => Options.MaxDocsPerBatch;

    public static SubscriptionProcessor Create(SubscriptionConnectionBase connection)
    {
        if (connection is OrchestratedSubscriptionConnection orchestratedSubscription)
            return new OrchestratedSubscriptionProcessor(connection.TcpConnection.DatabaseContext.ServerStore, connection.TcpConnection.DatabaseContext, orchestratedSubscription);

        if (connection is SubscriptionConnectionForShard sharded)
            return CreateForSharded(sharded);

        if (connection is SubscriptionConnection subscriptionConnection)
        {
            var database = connection.TcpConnection.DocumentDatabase;
            var server = database.ServerStore;
            if (connection.Subscription.Revisions)
            {
                return new RevisionsDatabaseSubscriptionProcessor(server, database, subscriptionConnection);
            }

            return new DocumentsDatabaseSubscriptionProcessor(server, database, subscriptionConnection);
        }

        throw new ArgumentException($"Unknown connection type {connection.GetType().FullName}");
    }

    public static SubscriptionProcessor CreateForSharded(SubscriptionConnectionForShard connection)
    {
        var database = connection.TcpConnection.DocumentDatabase as ShardedDocumentDatabase;
        var server = database.ServerStore;

        if (connection.Subscription.Revisions)
        {
            return new ShardedRevisionsDatabaseSubscriptionProcessor(server, database, connection);
        }

        return new ShardedDocumentsDatabaseSubscriptionProcessor(server, database, connection);
    }

    protected SubscriptionProcessor(ServerStore server, SubscriptionConnectionBase connection)
    {
        Server = server;
        Connection = connection;
    }

    public virtual void InitializeProcessor()
    {
        Collection = Connection.Subscription.Collection;
        Options = Connection.Options;
        SubscriptionState = Connection.SubscriptionState;
        RemoteEndpoint = Connection.TcpConnection.TcpClient.Client.RemoteEndPoint;
    }

    public abstract IEnumerable<(Document Doc, Exception Exception)> GetBatch();

    public abstract Task<long> RecordBatch(string lastChangeVectorSentInThisBatch);

    public abstract Task AcknowledgeBatch(long batchId);

    protected ClusterOperationContext ClusterContext;
    protected IncludeDocumentsCommand IncludesCmd;

    public virtual IDisposable InitializeForNewBatch(
        ClusterOperationContext clusterContext,
        out SubscriptionIncludeCommands includesCommands)
    {
        ClusterContext = clusterContext;
        includesCommands = CreateIncludeCommands();
        IncludesCmd = includesCommands.IncludeDocumentsCommand;

        InitializeProcessor();
        return null;
    }

    public class SubscriptionIncludeCommands
    {
        public IncludeDocumentsCommand IncludeDocumentsCommand;
        public IncludeTimeSeriesCommand IncludeTimeSeriesCommand;
        public IncludeCountersCommand IncludeCountersCommand;
    }

    protected abstract SubscriptionIncludeCommands CreateIncludeCommands();

    public virtual void Dispose()
    {
    }
}
