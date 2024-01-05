namespace Sparrow.Server.Utils.DiskStatsGetter;

public record DiskStatsResult
{
    public double IoReadOperations { get; init; }
    public double IoWriteOperations { get; init;}
    public Size ReadThroughput { get; set; }
    public Size WriteThroughput { get; set; }
    public long? QueueLength { get; set; }
}
