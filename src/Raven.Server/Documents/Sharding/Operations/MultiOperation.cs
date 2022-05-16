using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
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

    public async Task<IOperationResult> WaitForCompletionAsync(CancellationToken token)
    {
        _progresses = new IOperationProgress[_operations.Count];

        var tasks = new List<Task<IOperationResult>>(_progresses.Length);

        foreach (var operation in _operations.Values)
            tasks.Add(operation.WaitForCompletionAsync());

        await Task.WhenAll(tasks).WithCancellation(token);

        return CreateOperationResult(tasks);
    }

    private static IOperationResult CreateOperationResult(IEnumerable<Task<IOperationResult>> tasks)
    {
        IOperationResult result = null;
        foreach (var task in tasks)
        {
            var r = task.Result;
            if (result == null)
            {
                if (r.CanMerge == false)
                    throw new NotSupportedException();

                result = r;
                continue;
            }

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
