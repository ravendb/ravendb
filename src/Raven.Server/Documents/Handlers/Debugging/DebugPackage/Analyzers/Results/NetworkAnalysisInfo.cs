using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;

public class NetworkAnalysisInfo : IDynamicJson
{
    public int TotalActiveTcpConnections { get; set; }
    
    public List<TcpConnections> TcpConnections { get; set; } = [];
    public List<NodeDebugHandler.PingResult> PingTestResults { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(TotalActiveTcpConnections)] = TotalActiveTcpConnections,
            [nameof(TcpConnections)] = new DynamicJsonArray(TcpConnections.Select(x => x.ToJson())),
            [nameof(PingTestResults)] = PingTestResults != null ? new DynamicJsonArray(PingTestResults.Select(x => x.ToJson())) : null
        };
    }
}

public class TcpConnections : IDynamicJson
{
    public string TcpState { get; set; }
    
    public int NumberOfConnectionsInState { get; set; }
    
    public Dictionary<string, int> TopConnectionsInState { get; set; } = [];
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(TcpState)] = TcpState,
            [nameof(NumberOfConnectionsInState)] = NumberOfConnectionsInState,
            [nameof(TopConnectionsInState)] = DynamicJsonValue.Convert(TopConnectionsInState)
        };
    }
}
