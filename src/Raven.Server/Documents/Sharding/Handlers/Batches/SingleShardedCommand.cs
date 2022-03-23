using System.IO;

namespace Raven.Server.Documents.Sharding.Handlers.Batches;

public class SingleShardedCommand
{
    public int ShardNumber;

    public Stream AttachmentStream;

    public Stream CommandStream;

    public int PositionInResponse;
}
