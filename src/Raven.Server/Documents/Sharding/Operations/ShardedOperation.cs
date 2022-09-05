using Raven.Server.Documents.Operations;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations;

public class ShardedOperation : AbstractOperation
{
    [JsonDeserializationIgnore]
    public DatabaseMultiOperation Operation;
}
