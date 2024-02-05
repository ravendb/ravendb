using System;

namespace Sparrow.Server.Utils.DiskStatsGetter;

internal record LinuxDiskStatsRawResult : IDiskStatsRawResult
{
    public long IoReadOperations { get; init; }
    public long IoWriteOperations { get; init; }
    public long ReadSectors { get; init; }
    public long WriteSectors { get; init; }
    public long? QueueLength { get; init; }
    public DateTime Time { get; init;}
}
