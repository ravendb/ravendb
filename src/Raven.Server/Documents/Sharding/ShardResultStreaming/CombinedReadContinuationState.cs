using System;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Sharding.ShardResultStreaming;

public class CombinedReadContinuationState : IDisposable
{
    private readonly ShardedContext _shardedContext;
    private readonly CombinedStreamResult _combinedStream;
    public ReadContinuationState[] States;
    public CancellationToken CancellationToken;

    public CombinedReadContinuationState(ShardedContext shardedContext, CombinedStreamResult combinedStream)
    {
        _shardedContext = shardedContext;
        _combinedStream = combinedStream;
    }

    public async ValueTask InitializeAsync(CancellationToken token)
    {
        var shards = _shardedContext.ShardCount;
        States = new ReadContinuationState[shards];
        for (int i = 0; i < shards; i++)
        {
            var contextPool = _shardedContext.RequestExecutors[i].ContextPool;
            var state = new ReadContinuationState(contextPool, _combinedStream.Results.Span[i], token);
            await state.InitializeAsync();
            States[i] = state;
        }

        CancellationToken = token;
    }

    public void Dispose()
    {
        foreach (var state in States)
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
