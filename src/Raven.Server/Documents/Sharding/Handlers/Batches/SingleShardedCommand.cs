using System.Collections.Generic;
using System.IO;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Batches;

public class SingleShardedCommand
{
    public string Id;

    public int ShardNumber;

    public Stream AttachmentStream;

    public Stream CommandStream;

    public int PositionInResponse;

    public virtual IEnumerable<SingleShardedCommand> Retry(ShardedDatabaseContext databaseContext, TransactionOperationContext context)
    {
        ShardNumber = databaseContext.RecalculateShardNumberFor(context, Id);
        yield return this;
    }
}
