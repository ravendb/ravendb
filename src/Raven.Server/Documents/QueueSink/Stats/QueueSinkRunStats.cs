using Raven.Client.Util;

namespace Raven.Server.Documents.QueueSink.Stats;

public class QueueSinkRunStats
{
    public Size CurrentlyAllocated;

    public int NumberOfConsumedMessages;

    public int ScriptErrorCount;

    public string BatchStopReason;

    public bool? SuccessfullyProcessed;

    public Size BatchSize;
}
