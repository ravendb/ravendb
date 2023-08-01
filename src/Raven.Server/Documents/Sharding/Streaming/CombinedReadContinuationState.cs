using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Sharding.Streaming;

public sealed class CombinedReadContinuationState : IDisposable
{
    private readonly ShardedDatabaseContext _databaseContext;
    private readonly CombinedStreamResult _combinedStream;
    public Dictionary<int, ReadContinuationState> States;
    public CancellationToken CancellationToken;

    public CombinedReadContinuationState(ShardedDatabaseContext databaseContext, CombinedStreamResult combinedStream)
    {
        _databaseContext = databaseContext;
        _combinedStream = combinedStream;
    }

    public async ValueTask InitializeAsync(CancellationToken token)
    {
        var shards = _databaseContext.ShardCount;
        States = new Dictionary<int, ReadContinuationState>(shards);
        foreach (var shardNumber in _databaseContext.ShardsTopology.Keys)
        {
            var contextPool = _databaseContext.ShardExecutor.GetRequestExecutorAt(shardNumber).ContextPool;
            var state = new ReadContinuationState(contextPool, _combinedStream.Results[shardNumber].Result, token);
            await state.InitializeAsync();
            States[shardNumber] = state;
        }

        CancellationToken = token;
    }

    public void Dispose()
    {
        foreach (var state in States.Values)
        {
            try
            {
                state.Dispose();
            }
            catch
            {
                //
            }
        }
    }
}
