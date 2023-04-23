using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Extensions;
using Raven.Client.Http;

namespace Raven.Server.Documents.Sharding.Operations;

public abstract class AbstractShardedMultiOperation
{
    protected readonly long Id;
    protected readonly ShardedDatabaseContext ShardedDatabaseContext;

    private readonly Action<IOperationProgress> _onProgress;

    protected readonly ConcurrentDictionary<ShardedDatabaseIdentifier, Operation> Operations;

    private Dictionary<int, IOperationProgress> _progresses;

    protected AbstractShardedMultiOperation(long id, ShardedDatabaseContext shardedDatabaseContext, Action<IOperationProgress> onProgress)
    {
        Id = id;
        _onProgress = onProgress;
        Operations = new ConcurrentDictionary<ShardedDatabaseIdentifier, Operation>();
        ShardedDatabaseContext = shardedDatabaseContext;
    }

    public abstract ValueTask<TResult> ExecuteCommandForShard<TResult>(RavenCommand<TResult> command, int shardNumber, CancellationToken token);

    public abstract Operation CreateOperationInstance(ShardedDatabaseIdentifier key, long operationId);

    public abstract ValueTask KillAsync(CancellationToken token);

    public void Watch<TOperationProgress>(ShardedDatabaseIdentifier key, Operation operation)
        where TOperationProgress : IOperationProgress, new()
    {
        Debug.Assert(_progresses == null);

        if (Operations.TryAdd(key, operation) == false)
            return;

        operation.OnProgressChanged += (_, progress) => OnProgressChanged(key, progress);

        void OnProgressChanged(ShardedDatabaseIdentifier k, IOperationProgress progress)
        {
            if (progress.CanMerge == false)
            {
                if (typeof(TOperationProgress).IsAssignableTo(typeof(IShardedOperationProgress)))
                {
                    var sp = new TOperationProgress() as IShardedOperationProgress;
                    sp?.Fill(progress, k.ShardNumber, k.NodeTag);
                    progress = sp;
                }

                NotifyAboutProgress(progress);
                return;
            }

            _progresses[k.ShardNumber] = progress;

            MaybeNotifyAboutProgress();
        }
    }

    private void MaybeNotifyAboutProgress()
    {
        IOperationProgress result = null;
        
        foreach (var shardNumber in ShardedDatabaseContext.ShardsTopology.Keys)
        {
            if(_progresses.ContainsKey(shardNumber) == false)
                continue;

            var progress = _progresses[shardNumber];
            if (progress == null)
                continue;

            if (result == null)
            {
                result = progress.Clone();
                continue;
            }

            result.MergeWith(progress);
        }

        NotifyAboutProgress(result);
    }

    private void NotifyAboutProgress(IOperationProgress progress)
    {
        _onProgress(progress);
    }

    public async Task<IOperationResult> WaitForCompletionAsync<TOrchestratorResult>(CancellationToken token)
        where TOrchestratorResult : IOperationResult, new()
    {
        _progresses = new Dictionary<int, IOperationProgress>(Operations.Count);

        var tasks = new Dictionary<ShardedDatabaseIdentifier, Task<IOperationResult>>(Operations.Count);

        foreach (var operation in Operations)
        {
            tasks.Add(operation.Key, operation.Value.WaitForCompletionAsync());
        }

        await Task.WhenAll(tasks.Values).WithCancellation(token);

        var result = new TOrchestratorResult();

        if (result is IShardedOperationResult shardedResult)
        {
            foreach (var task in tasks)
            {
                shardedResult.CombineWith(task.Value.Result, task.Key.ShardNumber, task.Key.NodeTag);
            }

            return shardedResult;
        }

        foreach (var task in tasks)
        {
            var r = task.Value.Result;

            if (r.CanMerge == false)
                throw new NotSupportedException();
            else
                result.MergeWith(r);
        }

        return result;
    }
}
