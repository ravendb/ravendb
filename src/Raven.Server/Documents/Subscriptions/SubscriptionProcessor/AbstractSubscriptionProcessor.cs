using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents.Subscriptions.SubscriptionProcessor;

public abstract class AbstractSubscriptionProcessor<TIncludesCommand> : IDisposable
    where TIncludesCommand : AbstractIncludeDocumentsCommand
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
        Logger = LoggingSource.Instance.GetLogger(databaseName, GetType().FullName);
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
    protected TIncludesCommand IncludesCmd;

    public virtual IDisposable InitializeForNewBatch(ClusterOperationContext clusterContext, out TIncludesCommand includesCommands, out ITimeSeriesIncludes timeSeriesIncludes, out ICounterIncludes counterIncludes)
    {
        InitializeProcessor();

        ClusterContext = clusterContext;
        var commands = CreateIncludeCommands();
        IncludesCmd = commands.IncludesCommand;

        includesCommands = commands.IncludesCommand;
        timeSeriesIncludes = commands.TimeSeriesIncludes;
        counterIncludes = commands.CounterIncludes;

        return null;
    }

    protected abstract (TIncludesCommand IncludesCommand, ITimeSeriesIncludes TimeSeriesIncludes, ICounterIncludes CounterIncludes) CreateIncludeCommands();

    public virtual void Dispose()
    {
    }
}
