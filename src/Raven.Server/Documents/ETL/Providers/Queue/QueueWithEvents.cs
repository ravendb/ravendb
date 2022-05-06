using System.Collections.Generic;
using Raven.Client.Documents.Operations.ETL.Queue;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public class QueueWithEvents : EtlQueue
{
    public readonly List<QueueItem> Inserts = new List<QueueItem>();

    public QueueWithEvents(EtlQueue queue)
    {
        Name = queue.Name;
    }
}
