using Sparrow;

namespace Raven.Server.Documents.CdcSink.Stats;

public class CdcSinkRunStats
{
    public Size CurrentlyAllocated;

    public int NumberOfReadMessages;

    public int NumberOfProcessedMessages;

    public int ReadErrorCount;

    public int ScriptProcessingErrorCount;

    public string BatchPullStopReason;
}
