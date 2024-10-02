using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Http;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Changes;

internal sealed class ShardedDatabaseChanges : AbstractDatabaseChanges<ShardedDatabaseConnectionState>, IShardedDatabaseChanges
{
    private readonly ShardedChangesClientConnection _shardedChangesClientConnection;

    public ShardedDatabaseChanges(ShardedChangesClientConnection shardedChangesClientConnection, ServerStore server, RequestExecutor requestExecutor, string databaseName,
        Action onDispose, string nodeTag,
        bool throttleConnection)
        : base(requestExecutor, databaseName, onDispose, nodeTag, throttleConnection)
    {
        _shardedChangesClientConnection = shardedChangesClientConnection;
    }

    public async Task<IShardedDatabaseChanges> EnsureConnectedNow()
    {
        var changes = await EnsureConnectedNowAsync().ConfigureAwait(false);
        return changes as IShardedDatabaseChanges;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForDocument(string docId)
    {
        if (string.IsNullOrWhiteSpace(docId))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(docId));

        var counter = GetOrAddConnectionState("docs/" + docId, "watch-doc", "unwatch-doc", docId);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForAllDocuments()
    {
        var counter = GetOrAddConnectionState("all-docs", "watch-docs", "unwatch-docs", null);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForDocumentsStartingWith(string docIdPrefix)
    {
        if (string.IsNullOrWhiteSpace(docIdPrefix))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(docIdPrefix));

        var counter = GetOrAddConnectionState("prefixes/" + docIdPrefix, "watch-prefix", "unwatch-prefix", docIdPrefix);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForDocumentsInCollection(string collectionName)
    {
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(collectionName));

        var counter = GetOrAddConnectionState("collections/" + collectionName, "watch-collection", "unwatch-collection", collectionName);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForDocumentsInCollection<TEntity>()
    {
        throw new NotSupportedException("This is a Client API method only. Should not happen!");
    }

    public IChangesObservable<BlittableJsonReaderObject> ForAllCounters()
    {
        var counter = GetOrAddConnectionState("all-counters", "watch-counters", "unwatch-counters", null);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForCounter(string counterName)
    {
        if (string.IsNullOrWhiteSpace(counterName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(counterName));

        var counter = GetOrAddConnectionState($"counter/{counterName}", "watch-counter", "unwatch-counter", counterName);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForCounterOfDocument(string documentId, string counterName)
    {
        if (string.IsNullOrWhiteSpace(documentId))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(counterName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(counterName));

        var counter = GetOrAddConnectionState($"document/{documentId}/counter/{counterName}", "watch-document-counter", "unwatch-document-counter", value: null, values: new[] { documentId, counterName });

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForCountersOfDocument(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(documentId));

        var counter = GetOrAddConnectionState($"document/{documentId}/counter", "watch-document-counters", "unwatch-document-counters", documentId);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForIndex(string indexName)
    {
        if (string.IsNullOrWhiteSpace(indexName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(indexName));

        var counter = GetOrAddConnectionState("indexes/" + indexName, "watch-index", "unwatch-index", indexName);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForAllIndexes()
    {
        var counter = GetOrAddConnectionState("all-indexes", "watch-indexes", "unwatch-indexes", null);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForAllTimeSeries()
    {
        var counter = GetOrAddConnectionState("all-timeseries", "watch-all-timeseries", "unwatch-all-timeseries", null);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForTimeSeries(string timeSeriesName)
    {
        if (string.IsNullOrWhiteSpace(timeSeriesName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(timeSeriesName));

        var counter = GetOrAddConnectionState($"timeseries/{timeSeriesName}", "watch-timeseries", "unwatch-timeseries", timeSeriesName);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForTimeSeriesOfDocument(string documentId, string timeSeriesName)
    {
        if (string.IsNullOrWhiteSpace(documentId))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(documentId));
        if (string.IsNullOrWhiteSpace(timeSeriesName))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(timeSeriesName));

        var counter = GetOrAddConnectionState($"document/{documentId}/timeseries/{timeSeriesName}", "watch-document-timeseries", "unwatch-document-timeseries", value: null, values: new[] { documentId, timeSeriesName });

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForTimeSeriesOfDocument(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId))
            throw new ArgumentException("Value cannot be null or whitespace.", nameof(documentId));

        var counter = GetOrAddConnectionState($"document/{documentId}/timeseries", "watch-all-document-timeseries", "unwatch-all-document-timeseries", documentId);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    public IChangesObservable<BlittableJsonReaderObject> ForAggressiveCaching()
    {
        var counter = GetOrAddConnectionState("aggressive-caching", "watch-aggressive-caching", "unwatch-aggressive-caching", null);

        var taskedObservable = new ChangesObservable<BlittableJsonReaderObject, ShardedDatabaseConnectionState>(
            counter,
            filter: null);

        return taskedObservable;
    }

    protected override ShardedDatabaseConnectionState CreateDatabaseConnectionState(Func<Task> onConnect, Func<Task> onDisconnect) => new(onConnect, onDisconnect);

    protected override void ProcessNotification(string type, BlittableJsonReaderObject change)
    {
        switch (type)
        {
            case nameof(TopologyChange):
                change.TryGet("Value", out BlittableJsonReaderObject value);
                var topologyChange = TopologyChange.FromJson(value);

                var requestExecutor = RequestExecutor;
                if (requestExecutor != null)
                {
                    var node = new ServerNode
                    {
                        Url = topologyChange.Url,
                        Database = topologyChange.Database
                    };

                    requestExecutor.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(node)
                    {
                        TimeoutInMs = 0,
                        ForceUpdate = true,
                        DebugTag = "topology-change-notification"
                    }).ConfigureAwait(false);
                }
                break;

            default:
                _shardedChangesClientConnection.OnNext(change);
                break;
        }
    }
}
