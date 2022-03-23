using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;

namespace Raven.Server.Documents.Sharding.Streaming;

public class CombinedStreamResult
{
    public Memory<StreamResult> Results;

    public async ValueTask<CombinedReadContinuationState> InitializeAsync(ShardedDatabaseContext databaseContext, CancellationToken token)
    {
        var state = new CombinedReadContinuationState(databaseContext, this);
        await state.InitializeAsync(token);
        return state;
    }
}
