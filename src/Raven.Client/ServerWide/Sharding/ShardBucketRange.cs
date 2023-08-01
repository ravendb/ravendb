using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Sharding;

public sealed class ShardBucketRange
{
    public int BucketRangeStart;

    public int ShardNumber;

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(BucketRangeStart)] = BucketRangeStart, 
            [nameof(ShardNumber)] = ShardNumber
        };
    }
}
