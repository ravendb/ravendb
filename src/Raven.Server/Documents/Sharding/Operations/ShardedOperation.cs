using Raven.Server.Documents.Operations;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations;

public sealed class ShardedOperation : AbstractOperation
{
    [JsonDeserializationIgnore]
    public ShardedDatabaseMultiOperation Operation;
}
