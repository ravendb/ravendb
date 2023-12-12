using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Server.ServerWide.Commands;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide;
public class ClusterTransactionResult : IDynamicJsonValueConvertible
{
    public DynamicJsonArray GeneratedResult { get; set; }
    public List<ClusterTransactionCommand.ClusterTransactionErrorInfo> Errors { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(GeneratedResult)] = GeneratedResult,
            [nameof(Errors)] = Errors
        };
    }
}
