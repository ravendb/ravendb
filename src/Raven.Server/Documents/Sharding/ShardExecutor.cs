using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding
{
    public class ShardExecutor<T> : IDisposable
    {
        private readonly ShardedContext _shardedContext;
        private readonly CommandHolder[] _commands;
        private readonly IShardedOperation<T> _operation;

        private Dictionary<int, Exception> _exceptions;

        public ShardExecutor(ShardedContext shardedContext, IShardedOperation<T> operation) 
        {
            _shardedContext = shardedContext;
            _operation = operation;
            _commands = ArrayPool<CommandHolder>.Shared.Rent(_shardedContext.ShardCount);
        }

        public Task<T> ExecuteForAllAsync(ExecutionMode executionMode, FailureMode failureMode) => ExecuteForShardsAsync(Enumerable.Range(0, _shardedContext.ShardCount), executionMode, failureMode);

        public async Task<T> ExecuteForShardsAsync(IEnumerable<int> shards, ExecutionMode executionMode, FailureMode failureMode)
        {
            var position = 0;
            foreach (int shard in shards)
            {
                var cmd = _operation.CreateCommandForShard(shard);
                _commands[position].Shard = shard;
                _commands[position].Command = cmd;

                var executor = _shardedContext.RequestExecutors[shard];
                var release = executor.ContextPool.AllocateOperationContext(out JsonOperationContext ctx);
                _commands[position].ContextReleaser = release;

                var t = executor.ExecuteAsync(cmd, ctx);
                _commands[position].Task = t;

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

                position++;
            }

            for (var i = 0; i < position; i++)
            {
                var holder = _commands[i];
                try
                {
                    await holder.Task;
                }
                catch (Exception e)
                {
                    if (failureMode == FailureMode.Throw)
                        throw;

                    _exceptions ??= new Dictionary<int, Exception>();
                    _exceptions[holder.Shard] = e;
                }
            }

            var resultsArray = ArrayPool<T>.Shared.Rent(position);
            try
            {
                for (int i = 0; i < position; i++)
                {
                    resultsArray[i] = _commands[i].Command.Result;
                }

                return _operation.Combine(new Memory<T>(resultsArray, 0, position));
            }
            finally
            {
                ArrayPool<T>.Shared.Return(resultsArray);
            }
        }

        public void Dispose()
        {
            if (_commands != null)
            {
                for (var index = 0; index < _commands.Length; index++)
                {
                    var command = _commands[index];
                    try
                    {
                        command.ContextReleaser?.Dispose();
                        command.ContextReleaser = null; // we set it to null, since we pool it and might get old values if not cleared
                    }
                    catch
                    {
                        // ignore
                    }
                }

                ArrayPool<CommandHolder>.Shared.Return(_commands);
            }
        }

        private struct CommandHolder
        {
            public int Shard;
            public RavenCommand<T> Command;
            public Task Task;
            public IDisposable ContextReleaser;
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
