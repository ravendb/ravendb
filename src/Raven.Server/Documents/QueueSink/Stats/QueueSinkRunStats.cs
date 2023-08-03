
using Sparrow;

namespace Raven.Server.Documents.QueueSink.Stats;

public class QueueSinkRunStats
{
    public Size CurrentlyAllocated;

    public int NumberOfPulledMessages;

    public int NumberOfProcessedMessages;

    public int ScriptErrorCount;

    public string BatchPullStopReason;

    public bool? SuccessfullyProcessed;
}
