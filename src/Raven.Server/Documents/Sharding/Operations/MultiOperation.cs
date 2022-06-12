using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Extensions;

namespace Raven.Server.Documents.Sharding.Operations;

public class MultiOperation
{
    private readonly object _locker = new();

    private readonly long _id;
    private readonly ShardedDatabaseContext _context;

    private readonly Action<IOperationProgress> _onProgress;

    private readonly Dictionary<ShardedDatabaseIdentifier, Operation> _operations;

    private IOperationProgress[] _progresses;

    public MultiOperation(long id, ShardedDatabaseContext context, Action<IOperationProgress> onProgress)
    {
        _id = id;
        _context = context;
        _onProgress = onProgress;
        _operations = new Dictionary<ShardedDatabaseIdentifier, Operation>();
    }

    public void Watch(ShardedDatabaseIdentifier key, Operation operation)
    {
        Debug.Assert(_progresses == null);

        operation.OnProgressChanged += (_, progress) => OnProgressChanged(key, progress);

        lock (_locker)
        {
            _operations.Add(key, operation);
        }

        void OnProgressChanged(ShardedDatabaseIdentifier k, IOperationProgress progress)
        {
            if (progress.CanMerge == false)
            {
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
        _progresses = new IOperationProgress[_operations.Count];

        var tasks = new Dictionary<ShardedDatabaseIdentifier, Task<IOperationResult>>(_progresses.Length);

        foreach (var operation in _operations)
        {
            tasks.Add(operation.Key, operation.Value.WaitForCompletionAsync());
        }

        await Task.WhenAll(tasks.Values).WithCancellation(token);

        var result = new TOrchestratorResult();
        if (result is IShardedOperationResult shardedResult)
        {
            shardedResult.Results = new IShardNodeIdentifier[_operations.Count]; //TODO stav: change to dict in case of shard numbers not matching count
            foreach (var task in tasks)
            {
                shardedResult.CombineWith(task.Value.Result, task.Key.ShardNumber, task.Key.NodeTag);
            }
        }

        foreach (var task in tasks)
        {
            var r = task.Value.Result;
            
            if(r.CanMerge == false)
                throw new NotSupportedException();
            else
                result.MergeWith(r);
        }

        return result;
    }

    public async ValueTask KillAsync(bool waitForCompletion, CancellationToken token)
    {
        var tasks = new List<Task>(_operations.Count);
        foreach (var key in _operations.Keys)
            tasks.Add(_context.ShardExecutor.ExecuteSingleShardAsync(new KillOperationCommand(_id, key.NodeTag), key.ShardNumber, token));

        if (waitForCompletion)
            await Task.WhenAll(tasks).WithCancellation(token);
    }
}
