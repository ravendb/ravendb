using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Server.Documents.Sharding.Executors;

namespace Raven.Server.Documents.Sharding.Streaming;

public sealed class CombinedStreamResult
{
    public Dictionary<int, ShardExecutionResult<StreamResult>> Results;

    public async ValueTask<CombinedReadContinuationState> InitializeAsync(ShardedDatabaseContext databaseContext, CancellationToken token)
    {
        var state = new CombinedReadContinuationState(databaseContext, this);
        await state.InitializeAsync(token);
        return state;
    }
}
