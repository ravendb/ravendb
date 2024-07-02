using System.Collections.Generic;
using System.Text;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Sharding;

public sealed class PrefixedShardingSetting
{
    public string Prefix { get; set; }

    private byte[] _prefixBytesLowerCase;

    public PrefixedShardingSetting()
    {
    }

    public PrefixedShardingSetting(string prefix)
    {
        Prefix = prefix;
    }

    internal byte[] PrefixBytesLowerCase => _prefixBytesLowerCase ??= Encoding.UTF8.GetBytes(Prefix.ToLower());

    public List<int> Shards { get; set; }

    [ForceJsonSerialization]
    internal int BucketRangeStart { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(Prefix)] = Prefix, 
            [nameof(Shards)] = new DynamicJsonArray(Shards), 
            [nameof(BucketRangeStart)] = BucketRangeStart
        };
    }
}
