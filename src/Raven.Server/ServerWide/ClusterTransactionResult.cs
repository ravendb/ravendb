using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide;
public class ClusterTransactionResult : IDynamicJsonValueConvertible
{
    public DynamicJsonArray GeneratedResult { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(GeneratedResult)] = GeneratedResult,
        };
    }
}
