using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding
{
    public class ShardExecutor<T> : IDisposable
    {
        private readonly ShardedContext _shardedContext;
        private readonly Func<RavenCommand<T>> _commandFactory;

        private readonly Dictionary<int, Exception> _exceptions = new Dictionary<int, Exception>();
        private readonly Dictionary<int, RavenCommand<T>> _commands = new Dictionary<int, RavenCommand<T>>();
        private readonly List<IDisposable> _disposables = new List<IDisposable>();

        public ShardExecutor(ShardedContext shardedContext, Func<RavenCommand<T>> commandFactory)
        {
            _shardedContext = shardedContext;
            _commandFactory = commandFactory;
        }

        public Task ExecuteAsync(ExecutionMode executionMode, FailureMode failureMode) => ExecuteForShardsAsync(Enumerable.Range(0, _shardedContext.ShardCount), executionMode, failureMode);

        public async Task ExecuteForShardsAsync(IEnumerable<int> shards, ExecutionMode executionMode, FailureMode failureMode)
        {
            var tasks = new Dictionary<int, Task>();
            foreach (int shard in shards)
            {
                var cmd = _commandFactory();
                _commands[shard] = cmd;

                var executor = _shardedContext.RequestExecutors[shard];
                var release = executor.ContextPool.AllocateOperationContext(out JsonOperationContext ctx);
                _disposables.Add(release);

                var t = executor.ExecuteAsync(cmd, ctx);
                tasks[shard] = t;

                if (executionMode == ExecutionMode.OneByOne)
                {
                    try
                    {
                        await t;
                    }
                    catch
                    {
                        if (failureMode == FailureMode.Throw)
                            throw;
                    }
                }
            }

            try
            {
                await Task.WhenAll(tasks.Values);
            }
            catch
            {
                if (failureMode == FailureMode.Throw)
                    throw;

                foreach (var task in tasks)
                {
                    try
                    {
                        await task.Value;
                    }
                    catch (Exception e)
                    {
                        _exceptions[task.Key] = e;
                    }
                }
            }
        }

        public void CombineResults(Action<T> combineResultsAction)
        {
            foreach (var shardedCommand in _commands)
            {
                if (_exceptions.ContainsKey(shardedCommand.Key))
                    continue; // ignore faulted commands

                combineResultsAction(shardedCommand.Value.Result);
            }
        }

        public void Dispose()
        {
            foreach (var disposable in _disposables)
            {
                try
                {
                    disposable.Dispose();
                }
                catch
                {
                    // ignore
                }
            }
        }
    }
    
    public enum ExecutionMode
    {
        Parallel,
        OneByOne
    }

    public enum FailureMode
    {
        Throw,
        Ignore
    }
}
