using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Server.Documents.Includes.Sharding;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Sharding.Operations.Queries;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents.Sharding.Subscriptions;

public sealed class ShardedSubscriptionBatch : SubscriptionBatchBase<BlittableJsonReaderObject>
{
    public TaskCompletionSource SendBatchToClientTcs;
    public TaskCompletionSource ConfirmFromShardSubscriptionConnectionTcs;
    public string ShardName;
    private readonly ShardedDatabaseContext _databaseContext;
    public IDisposable ReturnContext;
    public JsonOperationContext Context;

    public void SetCancel()
    {
        SendBatchToClientTcs?.TrySetCanceled();
        ConfirmFromShardSubscriptionConnectionTcs?.TrySetCanceled();
    }

    public void SetException(Exception e)
    {
        SendBatchToClientTcs?.TrySetException(e);
        ConfirmFromShardSubscriptionConnectionTcs?.TrySetException(e);
    }

    public ShardedSubscriptionBatch(RequestExecutor requestExecutor, string dbName, Logger logger, ShardedDatabaseContext databaseContext) : base(requestExecutor, dbName, logger)
    {
        ShardName = dbName;
        _databaseContext = databaseContext;
    }

    protected override void EnsureDocumentId(BlittableJsonReaderObject item, string id) => throw new SubscriberErrorException($"Missing id property for {item}");

    internal override async ValueTask InitializeAsync(BatchFromServer batch)
    {
        SendBatchToClientTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ConfirmFromShardSubscriptionConnectionTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        ReturnContext = batch.ReturnContext;
        Context = batch.Context;
        batch.ReturnContext = null; // move the release responsibility to the OrchestratedSubscriptionProcessor
        batch.Context = null;


        await InitializeDocumentIncludesAsync(batch);
        await base.InitializeAsync(batch);

        LastSentChangeVectorInBatch = null;
    }

    internal async ValueTask InitializeDocumentIncludesAsync(BatchFromServer batchFromServer)
    {
        await TryGatherMissingDocumentIncludesAsync(batchFromServer.Includes);
        batchFromServer.Includes = _result?.Includes;
    }

    private ShardedQueryResult _result;
    private HashSet<string> _missingIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    private async ValueTask TryGatherMissingDocumentIncludesAsync(List<BlittableJsonReaderObject> list)
    {
        if (list == null || list.Count == 0)
            return;

        _result = new ShardedQueryResult
        {
            Results = new List<BlittableJsonReaderObject>(),
        };

        HashSet<string> missingDocumentIncludes = null;

        foreach (var includes in list)
        {
            _databaseContext.DatabaseShutdown.ThrowIfCancellationRequested();
            ShardedQueryOperation.HandleDocumentIncludesInternal(includes, Context, _result, ref missingDocumentIncludes);
        }

        if (missingDocumentIncludes == null)
            return;
        var processedIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await ShardedQueryProcessor.HandleMissingDocumentIncludesAsync(Context, request: null, _databaseContext, missingDocumentIncludes, _result, processedIds, metadataOnly: false, token: _databaseContext.DatabaseShutdown);

        if (missingDocumentIncludes.Count - processedIds.Count == 0)
        {
            // all missing includes exists and were processed
            return;
        }

        foreach (var id in missingDocumentIncludes)
        {
            if (processedIds.Contains(id))
            {
                continue;
            }

            // we have non-existing included document, need to return null for them to the actual client
            _result.Includes.Add(Context.ReadObject(new DynamicJsonValue
            {
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Id] = id,
                    [Constants.Documents.Metadata.Sharding.Subscription.NonPersistentFlags] = nameof(NonPersistentDocumentFlags.AllowDataAsNull)
                }
            }, id));
        }
    }

    public void CloneIncludes(ClusterOperationContext context, OrchestratorIncludesCommandImpl includes)
    {
        if (_includes != null)
            includes.IncludeDocumentsCommand.Gather(_includes);
        if (_counterIncludes != null)
            includes.IncludeCountersCommand.Gather(_counterIncludes, context);
        if (_timeSeriesIncludes != null)
            includes.IncludeTimeSeriesCommand.Gather(_timeSeriesIncludes, context);
    }
}
