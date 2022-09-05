using System;
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
    private readonly object _locker = new();

    protected readonly long Id;
    protected readonly ShardedDatabaseContext ShardedDatabaseContext;

    private readonly Action<IOperationProgress> _onProgress;

    protected readonly Dictionary<ShardedDatabaseIdentifier, Operation> Operations;

    private IOperationProgress[] _progresses;

    protected AbstractShardedMultiOperation(long id, ShardedDatabaseContext shardedDatabaseContext, Action<IOperationProgress> onProgress)
    {
        Id = id;
        _onProgress = onProgress;
        Operations = new Dictionary<ShardedDatabaseIdentifier, Operation>();
        ShardedDatabaseContext = shardedDatabaseContext;
    }

    public abstract ValueTask<TResult> ExecuteCommandForShard<TResult>(RavenCommand<TResult> command, int shardNumber, CancellationToken token);

    public abstract Operation CreateOperationInstance(ShardedDatabaseIdentifier key, long operationId);

    public abstract ValueTask KillAsync(CancellationToken token);

    public void Watch<TOperationProgress>(ShardedDatabaseIdentifier key, Operation operation)
        where TOperationProgress : IOperationProgress, new()
    {
        Debug.Assert(_progresses == null);

        operation.OnProgressChanged += (_, progress) => OnProgressChanged(key, progress);

        lock (_locker)
        {
            Operations.Add(key, operation);
        }

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

        for (var i = 0; i < _progresses.Length; i++)
        {
            var progress = _progresses[i];
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
        _progresses = new IOperationProgress[Operations.Count];

        var tasks = new Dictionary<ShardedDatabaseIdentifier, Task<IOperationResult>>(_progresses.Length);

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
