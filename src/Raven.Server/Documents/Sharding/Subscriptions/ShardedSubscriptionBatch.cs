using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Sharding.Subscriptions;

public class ShardedSubscriptionBatch : SubscriptionBatchBase<BlittableJsonReaderObject>
{
    public TaskCompletionSource SendBatchToClientTcs;
    public TaskCompletionSource ConfirmFromShardSubscriptionConnectionTcs;
    public string LastSentChangeVectorInBatch;
    public string ShardName;

    public ShardedSubscriptionBatch(RequestExecutor requestExecutor, string dbName, Logger logger) : base(requestExecutor, dbName, logger)
    {
        ShardName = dbName;
    }

    protected override void EnsureDocumentId(BlittableJsonReaderObject item, string id) => throw new SubscriberErrorException($"Missing id property for {item}");

    internal override string Initialize(BatchFromServer batch)
    {
        SendBatchToClientTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ConfirmFromShardSubscriptionConnectionTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        LastSentChangeVectorInBatch = null;
        return base.Initialize(batch);
    }
}
