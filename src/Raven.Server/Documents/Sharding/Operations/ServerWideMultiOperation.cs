using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Operations;
using Sparrow.Json;
using Operation = Raven.Client.Documents.Operations.Operation;

namespace Raven.Server.Documents.Sharding.Operations;

public class ServerWideMultiOperation : MultiOperation
{
    public ServerWideMultiOperation(long id, ShardedDatabaseContext context, Action<IOperationProgress> onProgress) : base(id, context, onProgress)
    {
    }

    public override async ValueTask<TResult> ExecuteCommandForShard<TResult>(RavenCommand<TResult> command, int shardNumber, CancellationToken token)
    {
        if (Context.ShardCount < shardNumber || Context.ShardsTopology[shardNumber].Members.Count == 0)
            throw new InvalidOperationException(
                $"Cannot execute command '{command.GetType()}' for Database '{Context.DatabaseName}${shardNumber}', shard {shardNumber} doesn't exist or has no members.");
        
        var nodeTag = Context.ShardsTopology[shardNumber].Members[0];
        var executor = Context.AllNodesExecutor.GetRequestExecutorForNode(nodeTag);
        using (executor.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
        {
            await executor.ExecuteAsync(command, ctx, token: token);
        }

        return command.Result;
    }

    public override Operation CreateOperationInstance(ShardedDatabaseIdentifier key, long operationId)
    {
        var executor = Context.AllNodesExecutor.GetRequestExecutorForNode(key.NodeTag);
        return new ServerWideOperation(executor, DocumentConventions.DefaultForServer, operationId, key.NodeTag);
    }

    public override async ValueTask KillAsync(CancellationToken token)
    {
        var tasks = new List<Task>(Operations.Count);
        using (Context.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext ctx))
        {
            foreach (var (key, operation) in Operations)
            {
                var command = new KillServerOperationCommand(operation.Id);
                var executor = Context.AllNodesExecutor.GetRequestExecutorForNode(key.NodeTag);
                
                tasks.Add(executor.ExecuteAsync(command, ctx, token: token));
            }

            await Task.WhenAll(tasks).WithCancellation(token);
        }

        var serverOperation = Context.ServerStore.Operations.GetOperation(Id);
        if (serverOperation == null)
            return;

        await serverOperation.KillAsync(waitForCompletion: true, token);
    }
}
