using System;
using System.Diagnostics;

namespace Sparrow.Server.Utils.DiskStatsGetter;

internal record WindowsDiskStatsRawResult : IDiskStatsRawResult
{
    public CounterSample IoReadOperations { get; init; }
    public CounterSample IoWriteOperations { get; init; }
    public CounterSample ReadThroughput { get; init; }
    public CounterSample WriteThroughput { get; init; }
    public CounterSample QueueLength { get; init; }
    public DateTime Time { get; init;}
}
