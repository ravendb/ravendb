using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Includes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor;

public abstract class AbstractSubscriptionProcessor<TIncludesCommand> : IDisposable
    where TIncludesCommand : AbstractIncludesCommand
{
    protected readonly ServerStore Server;
    protected readonly ISubscriptionConnection Connection;

    protected EndPoint RemoteEndpoint;
    protected SubscriptionState SubscriptionState;
    protected SubscriptionWorkerOptions Options;
    protected string Collection;

    protected int BatchSize => Options.MaxDocsPerBatch;

    protected Logger Logger;

    protected AbstractSubscriptionProcessor(ServerStore server, ISubscriptionConnection connection, string databaseName)
    {
        Server = server;
        Connection = connection;
        Logger = LoggingSource.Instance.GetLogger(databaseName, connection == null ? $"{nameof(TestDocumentsDatabaseSubscriptionProcessor)}" : $"{nameof(SubscriptionProcessor)}<{connection.Options.SubscriptionName}>");
    }

    public virtual void InitializeProcessor()
    {
        Collection = Connection.Subscription.Collection;
        Options = Connection.Options;
        SubscriptionState = Connection.SubscriptionState;
        RemoteEndpoint = Connection.TcpConnection.TcpClient.Client.RemoteEndPoint;
    }

    public abstract IEnumerable<(Document Doc, Exception Exception)> GetBatch();

    public bool IsActiveMigration;

    public abstract Task<long> RecordBatch(string lastChangeVectorSentInThisBatch);

    public abstract Task AcknowledgeBatch(long batchId, string changeVector);

    protected ClusterOperationContext ClusterContext;
    protected TIncludesCommand IncludesCmd;

    public virtual IDisposable InitializeForNewBatch(ClusterOperationContext clusterContext, out TIncludesCommand includesCommands)
    {
        ClusterContext = clusterContext;
        InitializeProcessor();

        var commands = CreateIncludeCommands();
        IncludesCmd = commands;
        includesCommands = commands;

        return null;
    }

    protected abstract TIncludesCommand CreateIncludeCommands();

    protected abstract ConflictStatus GetConflictStatus(string changeVector);

    public virtual void Dispose()
    {
    }
}
