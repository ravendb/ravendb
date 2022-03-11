using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;

namespace Raven.Server.Documents.Sharding.Streaming;

public class CombinedStreamResult
{
    public Memory<StreamResult> Results;

    public async ValueTask<CombinedReadContinuationState> InitializeAsync(ShardedContext shardedContext, CancellationToken token)
    {
        var state = new CombinedReadContinuationState(shardedContext, this);
        await state.InitializeAsync(token);
        return state;
    }
}
