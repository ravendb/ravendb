using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using static Raven.Server.ServerWide.Commands.ClusterTransactionCommand;

namespace Raven.Server.ServerWide;
public class ClusterTransactionResult : IDynamicJsonValueConvertible
{
    public DynamicJsonArray GeneratedResult { get; set; }
    public List<ClusterTransactionErrorInfo> Errors { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(GeneratedResult)] = GeneratedResult,
            [nameof(Errors)] = Errors
        };
    }
}
