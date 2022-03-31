using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Xunit.Sdk;

namespace Tests.Infrastructure.Utils;

public class MaintenanceOperationExecutorTester<TResult> : IMaintenanceOperationExecutorReadTester<TResult>, IMaintenanceOperationExecutorActionTester
{
    private readonly MaintenanceOperationExecutor _executor;
    private readonly Func<IMaintenanceOperation<TResult>> _factoryWithResult;
    private readonly Func<IMaintenanceOperation> _factoryWithoutResult;

    private DatabaseRecord _databaseRecord;

    public MaintenanceOperationExecutorTester(MaintenanceOperationExecutor executor, Func<IMaintenanceOperation<TResult>> factory)
    {
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        _factoryWithResult = factory ?? throw new ArgumentNullException(nameof(factory));
    }

    public MaintenanceOperationExecutorTester(MaintenanceOperationExecutor executor, Func<IMaintenanceOperation> factory)
    {
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        _factoryWithoutResult = factory ?? throw new ArgumentNullException(nameof(factory));
    }

    public async Task ExecuteOnAllAsync()
    {
        await foreach (var _ in ExecuteAsync())
        {
            // just executing
        }
    }

    public void ExecuteOnAll()
    {
        AsyncHelpers.RunSync(ExecuteOnAllAsync);
    }

    public async Task AssertAllAsync(Action<Key, TResult> assert)
    {
        await foreach (var (key, result) in GetResultsAsync())
        {
            try
            {
                assert(key, result);
            }
            catch (XunitException e)
            {
                throw new InvalidOperationException($"Assertion failed for '{key}'.", e);
            }
        }
    }

    public void AssertAll(Action<Key, TResult> assert)
    {
        AsyncHelpers.RunSync(() => AssertAllAsync(assert));
    }

    private async IAsyncEnumerable<(Key Key, TResult Result)> GetResultsAsync()
    {
        _databaseRecord ??= await _executor.Server.SendAsync(new GetDatabaseRecordOperation(_executor._databaseName));
        if (_databaseRecord.IsSharded)
        {
            for (var i = 0; i < _databaseRecord.Shards.Length; i++)
            {
                var shardTopology = _databaseRecord.Shards[i];

                foreach (var (nKey, nExecutor) in GetExecutors(shardTopology))
                {
                    var shardKey = nKey.ForShard(i);
                    var shardExecutor = nExecutor.ForShard(i);

                    yield return (shardKey, await shardExecutor.SendAsync(_factoryWithResult()));
                }
            }

            yield break;
        }

        foreach (var (key, executor) in GetExecutors(_databaseRecord.Topology))
        {
            yield return (key, await executor.SendAsync(_factoryWithResult()));
        }
    }

    private async IAsyncEnumerable<Key> ExecuteAsync()
    {
        _databaseRecord ??= await _executor.Server.SendAsync(new GetDatabaseRecordOperation(_executor._databaseName));
        if (_databaseRecord.IsSharded)
        {
            for (var i = 0; i < _databaseRecord.Shards.Length; i++)
            {
                var shardTopology = _databaseRecord.Shards[i];

                foreach (var (nKey, nExecutor) in GetExecutors(shardTopology))
                {
                    var shardKey = nKey.ForShard(i);
                    var shardExecutor = nExecutor.ForShard(i);
                    await shardExecutor.SendAsync(_factoryWithoutResult());

                    yield return shardKey;
                }
            }

            yield break;
        }

        foreach (var (key, executor) in GetExecutors(_databaseRecord.Topology))
        {
            await executor.SendAsync(_factoryWithoutResult());
            yield return key;
        }
    }

    private IEnumerable<(Key Key, MaintenanceOperationExecutor Executor)> GetExecutors(DatabaseTopology topology)
    {
        foreach (string member in topology.Members)
        {
            var key = new Key(member);
            var executor = _executor.ForNode(member);

            yield return (key, executor);
        }
    }

    public class Key
    {
        public string NodeTag { get; }

        public int? ShardNumber { get; }

        public Key(string nodeTag)
        {
            NodeTag = nodeTag ?? throw new ArgumentNullException(nameof(nodeTag));
        }

        private Key(string nodeTag, int shardNumber)
        {
            NodeTag = nodeTag;
            ShardNumber = shardNumber;
        }

        public Key ForShard(int shardNumber)
        {
            return new Key(NodeTag, shardNumber);
        }

        public override string ToString()
        {
            var builder = new StringBuilder();
            builder.Append($"{nameof(NodeTag)} = {NodeTag}");
            if (ShardNumber != null)
                builder.Append($", {nameof(ShardNumber)} = {ShardNumber}");

            return builder.ToString();
        }
    }
}

public interface IMaintenanceOperationExecutorActionTester
{
    Task ExecuteOnAllAsync();

    void ExecuteOnAll();
}

public interface IMaintenanceOperationExecutorReadTester<TResult>
{
    Task AssertAllAsync(Action<MaintenanceOperationExecutorTester<TResult>.Key, TResult> assert);

    void AssertAll(Action<MaintenanceOperationExecutorTester<TResult>.Key, TResult> assert);
}
