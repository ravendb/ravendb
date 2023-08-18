
using Sparrow;

namespace Raven.Server.Documents.QueueSink.Stats;

public class QueueSinkRunStats
{
    public Size CurrentlyAllocated;

    public int NumberOfReadMessages;

    public int NumberOfProcessedMessages;

    public int ReadErrorCount;

    public int ScriptProcessingErrorCount;

    public string BatchPullStopReason;
}
