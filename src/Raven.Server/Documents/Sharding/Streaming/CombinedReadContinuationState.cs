using System;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Sharding.Streaming;

public class CombinedReadContinuationState : IDisposable
{
    private readonly ShardedDatabaseContext _databaseContext;
    private readonly CombinedStreamResult _combinedStream;
    public ReadContinuationState[] States;
    public CancellationToken CancellationToken;

    public CombinedReadContinuationState(ShardedDatabaseContext databaseContext, CombinedStreamResult combinedStream)
    {
        _databaseContext = databaseContext;
        _combinedStream = combinedStream;
    }

    public async ValueTask InitializeAsync(CancellationToken token)
    {
        var shards = _databaseContext.ShardCount;
        States = new ReadContinuationState[shards];
        for (int i = 0; i < shards; i++)
        {
            var contextPool = _databaseContext.ShardExecutor.GetRequestExecutorAt(i).ContextPool;
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
