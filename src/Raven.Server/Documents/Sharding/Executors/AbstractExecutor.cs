using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Maintenance.Sharding;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Executors;

public sealed class ShardExecutionResult<T>
{
    public int ShardNumber;
    public RavenCommand<T> Command;
    public T Result;
    public Task CommandTask;
    public IDisposable ContextReleaser;
}

public abstract class AbstractExecutor : IDisposable
{
    protected readonly ServerStore ServerStore;

    protected AbstractExecutor([NotNull] ServerStore store)
    {
        ServerStore = store ?? throw new ArgumentNullException(nameof(store));
        ServerStore.Server.ServerCertificateChanged += OnCertificateChange;
    }

    public void ForgetAbout()
    {
        // the event handler holds a strong reference to this object, so it will not be collected by the gc
        ServerStore.Server.ServerCertificateChanged -= OnCertificateChange;
    }

    public abstract RequestExecutor GetRequestExecutorAt(int position);

    protected abstract Memory<int> GetAllPositions();

    protected abstract void OnCertificateChange(object sender, EventArgs e);

    public Task<TCombinedResult> ExecuteParallelForAllAsync<TResult, TCombinedResult>(IShardedOperation<TResult, TCombinedResult> operation, CancellationToken token = default)
        => ExecuteForShardsAsync<ParallelExecution, ThrowOnFirstFailure, TResult, TCombinedResult>(GetAllPositions(), operation, token);

    public Task<TCombinedResult> ExecuteParallelForAllThrowAggregatedFailure<TResult, TCombinedResult>(IShardedOperation<TResult, TCombinedResult> operation, CancellationToken token = default)
        => ExecuteForShardsAsync<ParallelExecution, ThrowAggregatedFailure, TResult, TCombinedResult>(GetAllPositions(), operation, token);

    public Task<TResult> ExecuteParallelForAllAsync<TResult>(IShardedOperation<TResult> operation, CancellationToken token = default)
        => ExecuteForShardsAsync<ParallelExecution, ThrowOnFirstFailure, TResult>(GetAllPositions(), operation, token);

    protected Task<TResult> ExecuteForShardsAsync<TExecutionMode, TFailureMode, TResult>(Memory<int> shards, IShardedOperation<TResult, TResult> operation, CancellationToken token = default)
        where TExecutionMode : struct, IExecutionMode
        where TFailureMode : struct, IFailureMode
        => ExecuteForShardsAsync<TExecutionMode, TFailureMode, TResult, TResult>(shards, operation, token);

    public Task ExecuteParallelForShardsAsync(Memory<int> shards,
        IShardedOperation operation, CancellationToken token = default)
        => ExecuteForShardsAsync<ParallelExecution, ThrowOnFirstFailure, object, object>(shards, operation, token);

    public Task<TResult> ExecuteParallelForShardsAsync<TResult>(Memory<int> shards,
        IShardedOperation<TResult> operation, CancellationToken token = default)
        => ExecuteForShardsAsync<ParallelExecution, ThrowOnFirstFailure, TResult, TResult>(shards, operation, token);

    public Task<TCombinedResult> ExecuteParallelForShardsAsync<TResult, TCombinedResult>(Memory<int> shards,
        IShardedOperation<TResult, TCombinedResult> operation, CancellationToken token = default)
        => ExecuteForShardsAsync<ParallelExecution, ThrowOnFirstFailure, TResult, TCombinedResult>(shards, operation, token);

    public Task<TCombinedResult> ExecuteParallelAndIgnoreErrorsForShardsAsync<TResult, TCombinedResult>(Memory<int> shards,
        IShardedOperation<TResult, TCombinedResult> operation, CancellationToken token = default)
        => ExecuteForShardsAsync<ParallelExecution, IgnoreFailure, TResult, TCombinedResult>(shards, operation, token);

    protected async Task<TCombinedResult> ExecuteForShardsAsync<TExecutionMode, TFailureMode, TResult, TCombinedResult>(Memory<int> shards,
        IShardedOperation<TResult, TCombinedResult> operation, CancellationToken token)
        where TExecutionMode : struct, IExecutionMode
        where TFailureMode : struct, IFailureMode
    {
        var commands = new Dictionary<int, ShardExecutionResult<TResult>>();
        try
        {
            await ExecuteAsync<TExecutionMode, TFailureMode, TResult, TCombinedResult>(shards, operation, commands, token);

            if (operation is IShardedOperation)
                return default;

            return BuildResults(operation, commands);
        }
        finally
        {
            foreach (var (shardNumber, command) in commands)
            {
                try
                {
                    if (command.CommandTask.IsCompleted == false)
                    {
                        // we must not return the context if a command task is still running so we need to await it
                        // this can happen when ThrowOnFailure mode is used and we throw on first failure

                        try
                        {
                            await command.CommandTask;
                        }
                        catch
                        {
                            // ignore
                        }
                    }

                    command.ContextReleaser?.Dispose();
                    command.ContextReleaser = null; // we set it to null, since we pool it and might get old values if not cleared
                }
                catch
                {
                    // ignore
                }
            }
        }
    }

    private static TCombinedResult BuildResults<TResult, TCombinedResult>(
        IShardedOperation<TResult, TCombinedResult> operation,
        Dictionary<int, ShardExecutionResult<TResult>> commands)
    {
        foreach (var holder in commands.Values)
        {
            holder.Result = holder.Command.Result;
        }

        var result = operation.CombineCommands(commands);

        if (typeof(TCombinedResult) == typeof(BlittableJsonReaderObject))
        {
            if (result == null)
                return default;

            var blittable = result as BlittableJsonReaderObject;
            return (TCombinedResult)(object)blittable.Clone(operation.CreateOperationContext());
        }

        return result;
    }

    private async Task ExecuteAsync<TExecutionMode, TFailureMode, TResult, TCombinedResult>(
        Memory<int> shards,
        IShardedOperation<TResult, TCombinedResult> operation,
        Dictionary<int, ShardExecutionResult<TResult>> commands,
        CancellationToken token)

        where TExecutionMode : struct, IExecutionMode
        where TFailureMode : struct, IFailureMode
    {
        for (int position = 0; position < shards.Span.Length; position++)
        {
            int shardNumber = shards.Span[position];

            var cmd = operation.CreateCommandForShard(shardNumber);
            if (cmd == null)
                continue;

            cmd.ModifyRequest = operation.ModifyHeaders;
            cmd.ModifyUrl = operation.ModifyUrl;
            
            var executor = GetRequestExecutorAt(shardNumber);
            var release = executor.ContextPool.AllocateOperationContext(out JsonOperationContext ctx);
            
            var t = executor.ExecuteAsync(cmd, ctx, token: token);
            
            commands.Add(shardNumber, new ShardExecutionResult<TResult>()
            {
                ShardNumber = shardNumber,
                Command = cmd,
                CommandTask = t,
                ContextReleaser = release
            });

            if (typeof(TExecutionMode) == typeof(OneByOneExecution))
            {
                try
                {
                    await t;
                }
                catch
                {
                    if (typeof(TFailureMode) == typeof(ThrowOnFirstFailure))
                        throw;
                }
            }
        }

        List<ExecutionForShardException> exceptions = null;

        foreach (var (shardNumber, command) in commands)
        {
            try
            {
                await command.CommandTask;
            }
            catch (Exception e)
            {
                if (typeof(TFailureMode) == typeof(ThrowOnFirstFailure))
                    throw;

                exceptions ??= new List<ExecutionForShardException>();
                exceptions.Add(new ExecutionForShardException(shardNumber, e));
            }
        }

        if (typeof(TFailureMode) == typeof(ThrowAggregatedFailure) && exceptions?.Count > 0)
        {
            if (exceptions.Count == 1)
                throw exceptions.First();

            throw new AggregateException($"Some shards threw an exception", exceptions);
        }

    }

    protected static void SafelyDisposeExecutors(IEnumerable<RequestExecutor> executors)
    {
        foreach (var disposable in executors)
        {
            try
            {
                disposable.Dispose();
            }
            catch
            {
                // ignored
            }
        }
    }

    protected static void SafelyDisposeExecutors(IEnumerable<Lazy<RequestExecutor>> executors)
    {
        foreach (var disposable in executors)
        {
            try
            {
                if (disposable.IsValueCreated == false)
                    continue;

                disposable.Value.Dispose();
            }
            catch
            {
                // ignored
            }
        }
    }

    public virtual void Dispose()
    {
        GC.SuppressFinalize(this);
    }

    ~AbstractExecutor()
    {
        try
        {
            Dispose();
        }
#pragma warning disable CS0168
        catch (Exception e)
#pragma warning restore CS0168
        {
#if DEBUG
            Console.WriteLine($"Finalizer of {GetType()} got an exception:{Environment.NewLine}" + e);
#endif
            // nothing we can do here
        }
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

public struct ThrowOnFirstFailure : IFailureMode
{

}

public struct IgnoreFailure : IFailureMode
{

}

public struct ThrowAggregatedFailure : IFailureMode
{

}
