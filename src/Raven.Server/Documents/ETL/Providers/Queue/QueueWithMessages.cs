using System.Collections.Generic;
using Raven.Client.Documents.Operations.ETL.Queue;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public class QueueWithMessages : EtlQueue
{
    public readonly List<QueueItem> Messages = new();

    public QueueWithMessages(EtlQueue queue)
    {
        Name = queue.Name;
        DeleteProcessedDocuments = queue.DeleteProcessedDocuments;
    }
}
