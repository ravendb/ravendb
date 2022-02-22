using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding
{
    public class ShardExecutor
    {
        private readonly ShardedContext _shardedContext;
        private Dictionary<int, Exception> _exceptions;

        public ShardExecutor(ShardedContext shardedContext) 
        {
            _shardedContext = shardedContext;
        }

        public Task<TResult> ExecuteOneByOneForAllAsync<TResult>(IShardedOperation<TResult> operation)
            => ExecuteForShardsAsync<OneByOneExecution, ThrowOnFailure, TResult>(new Memory<int>(_shardedContext.FullRange), operation);

        public Task<TCombinedResult> ExecuteParallelForAllAsync<TResult, TCombinedResult>(IShardedOperation<TResult, TCombinedResult> operation)
            => ExecuteForShardsAsync<ParallelExecution, ThrowOnFailure, TResult, TCombinedResult>(new Memory<int>(_shardedContext.FullRange), operation);

        public Task<TResult> ExecuteParallelForAllAsync<TResult>(IShardedOperation<TResult> operation)
            => ExecuteForShardsAsync<ParallelExecution, ThrowOnFailure, TResult>(new Memory<int>(_shardedContext.FullRange), operation);

        public Task<TResult> ExecuteForAllAsync<TExecutionMode, TFailureMode, TResult>(IShardedOperation<TResult> operation)
            where TExecutionMode : struct, IExecutionMode
            where TFailureMode : struct, IFailureMode
            => ExecuteForShardsAsync<TExecutionMode, TFailureMode, TResult>(new Memory<int>(_shardedContext.FullRange), operation);

        public Task<TResult> ExecuteForShardsAsync<TExecutionMode, TFailureMode, TResult>(Memory<int> shards, IShardedOperation<TResult, TResult> operation)
            where TExecutionMode : struct, IExecutionMode
            where TFailureMode : struct, IFailureMode
            => ExecuteForShardsAsync<TExecutionMode, TFailureMode, TResult, TResult>(shards, operation);

        public async Task<TCombinedResult> ExecuteForShardsAsync<TExecutionMode, TFailureMode, TResult, TCombinedResult>(Memory<int> shards, IShardedOperation<TResult, TCombinedResult> operation)
            where TExecutionMode : struct, IExecutionMode
            where TFailureMode : struct, IFailureMode
        {
            int position = 0;
            var commands = ArrayPool<CommandHolder<TResult>>.Shared.Rent(shards.Length);
            try
            {
                for (position = 0; position < shards.Span.Length; position++)
                {
                    int shard = shards.Span[position];

                    var cmd = operation.CreateCommandForShard(shard);
                    commands[position].Shard = shard;
                    commands[position].Command = cmd;

                    var executor = _shardedContext.RequestExecutors[shard];
                    var release = executor.ContextPool.AllocateOperationContext(out JsonOperationContext ctx);
                    commands[position].ContextReleaser = release;

                    var t = executor.ExecuteAsync(cmd, ctx);
                    commands[position].Task = t;

                    if (typeof(TExecutionMode) == typeof(OneByOneExecution))
                    {
                        try
                        {
                            await t;
                        }
                        catch
                        {
                            if (typeof(TFailureMode) == typeof(ThrowOnFailure))
                                throw;
                        }
                    }
                }

                for (var i = 0; i < position; i++)
                {
                    var holder = commands[i];
                    try
                    {
                        await holder.Task;
                    }
                    catch (Exception e)
                    {
                        if (typeof(TFailureMode) == typeof(ThrowOnFailure))
                            throw;

                        _exceptions ??= new Dictionary<int, Exception>();
                        _exceptions[holder.Shard] = e;
                    }
                }

                var resultsArray = ArrayPool<TResult>.Shared.Rent(position);
                try
                {
                    for (int i = 0; i < position; i++)
                    {
                        resultsArray[i] = commands[i].Command.Result;
                    }

                    var result = operation.Combine(new Memory<TResult>(resultsArray, 0, position));
                    if (typeof(TCombinedResult) == typeof(BlittableJsonReaderObject))
                    {
                        if (result == null)
                            return default;

                        var blittable = result as BlittableJsonReaderObject;
                        return (TCombinedResult)(object)blittable.Clone(operation.CreateOperationContext());
                    }
                    return result;
                }
                finally
                {
                    ArrayPool<TResult>.Shared.Return(resultsArray);
                }
            }
            finally
            {
                if (commands != null)
                {
                    for (var index = 0; index < position; index++)
                    {
                        var command = commands[index];
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

                    ArrayPool<CommandHolder<TResult>>.Shared.Return(commands);
                }
            }
        }

        public struct CommandHolder<T>
        {
            public int Shard;
            public RavenCommand<T> Command;
            public Task Task;
            public IDisposable ContextReleaser;
        }
    }

    public interface IExecutionMode
    {

    }

    public interface IFailureMode
    {

    }

    public struct ParallelExecution : IExecutionMode
    {

    }

    public struct OneByOneExecution : IExecutionMode
    {

    }

    public struct ThrowOnFailure : IFailureMode
    {

    }

    public struct IgnoreFailure : IFailureMode
    {

    }
}
