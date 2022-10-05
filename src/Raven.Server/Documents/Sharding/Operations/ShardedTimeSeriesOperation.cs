using System;
using System.Collections.Generic;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Http;

namespace Raven.Server.Documents.Sharding.Operations;

internal class ShardedTimeSeriesOperation : IShardedOperation<GetMultipleTimeSeriesRangesCommand.Result>
{
    public HttpContext HttpContext { get; }

    private readonly Dictionary<int, GetMultipleTimeSeriesRangesCommand> _commandsPerShard;

    public ShardedTimeSeriesOperation(HttpContext httpContext, Dictionary<int, GetMultipleTimeSeriesRangesCommand> commandsPerShard)
    {
        HttpContext = httpContext;
        _commandsPerShard = commandsPerShard;
    }

    public HttpRequest HttpRequest { get; }

    public GetMultipleTimeSeriesRangesCommand.Result Combine(Memory<GetMultipleTimeSeriesRangesCommand.Result> results)
    {
        GetMultipleTimeSeriesRangesCommand.Result result = new()
        {
            Results = new List<TimeSeriesDetails>()
        };

        foreach (var cmdResult in results.Span)
        {
            result.Results.AddRange(cmdResult.Results);
        }

        return result;
    }

    RavenCommand<GetMultipleTimeSeriesRangesCommand.Result> IShardedOperation<GetMultipleTimeSeriesRangesCommand.Result, GetMultipleTimeSeriesRangesCommand.Result>.CreateCommandForShard(int shardNumber)
    {
        return _commandsPerShard[shardNumber];
    }
}
