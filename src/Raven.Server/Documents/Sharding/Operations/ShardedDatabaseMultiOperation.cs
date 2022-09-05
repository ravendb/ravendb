using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;

namespace Raven.Server.Documents.Sharding.Operations;

public class ShardedDatabaseMultiOperation : AbstractShardedMultiOperation
{
    public ShardedDatabaseMultiOperation(long id, ShardedDatabaseContext context, Action<IOperationProgress> onProgress) : base(id, context, onProgress)
    {
    }

    public override async ValueTask KillAsync(CancellationToken token)
    {
        var tasks = new List<Task>(Operations.Count);
        foreach (var (key, op) in Operations)
            tasks.Add(ShardedDatabaseContext.ShardExecutor.ExecuteSingleShardAsync(new KillOperationCommand(op.Id, key.NodeTag), key.ShardNumber, token));

        await Task.WhenAll(tasks);

        var shardedOperation = ShardedDatabaseContext.Operations.GetOperation(Id);
        if (shardedOperation == null)
            return;

        await shardedOperation.KillAsync(waitForCompletion: true, token);
    }

    public override async ValueTask<TResult> ExecuteCommandForShard<TResult>(RavenCommand<TResult> command, int shardNumber, CancellationToken token)
    {
        await ShardedDatabaseContext.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token);
        return command.Result;
    }

    public override Operation CreateOperationInstance(ShardedDatabaseIdentifier key, long operationId)
    {
        var changes = ShardedDatabaseContext.Operations.GetChanges(key);
        var requestExecutor = ShardedDatabaseContext.ShardExecutor.GetRequestExecutorAt(key.ShardNumber);

        return new Operation(requestExecutor, () => changes, DocumentConventions.DefaultForServer, operationId, key.NodeTag);
    }
}

