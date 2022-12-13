using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Executors;

namespace Raven.Server.Documents.Sharding.Operations;

internal class ShardedTimeSeriesOperation : IShardedOperation<GetMultipleTimeSeriesRangesCommand.Response>
{
    public HttpContext HttpContext { get; }

    private readonly Dictionary<int, GetMultipleTimeSeriesRangesCommand> _commandsPerShard;

    public ShardedTimeSeriesOperation(HttpContext httpContext, Dictionary<int, GetMultipleTimeSeriesRangesCommand> commandsPerShard)
    {
        HttpContext = httpContext;
        _commandsPerShard = commandsPerShard;
    }

    public HttpRequest HttpRequest { get; }

    public GetMultipleTimeSeriesRangesCommand.Response Combine(Dictionary<int, ShardExecutionResult<GetMultipleTimeSeriesRangesCommand.Response>> results)
    {
        GetMultipleTimeSeriesRangesCommand.Response result = new()
        {
            Results = new List<TimeSeriesDetails>()
        };

        foreach (var cmdResult in results.Values)
        {
            result.Results.AddRange(cmdResult.Result.Results);
        }

        return result;
    }

    RavenCommand<GetMultipleTimeSeriesRangesCommand.Response> IShardedOperation<GetMultipleTimeSeriesRangesCommand.Response, GetMultipleTimeSeriesRangesCommand.Response>.CreateCommandForShard(int shardNumber)
    {
        return _commandsPerShard[shardNumber];
    }
}
