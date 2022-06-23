using System.Collections.Generic;
using Raven.Client.Documents.Operations.ETL.Queue;

namespace Raven.Server.Documents.ETL.Providers.Queue;

public class QueueWithItems<T> : EtlQueue where T: QueueItem
{
    public readonly List<T> Items = new();

    public QueueWithItems(EtlQueue queue)
    {
        Name = queue.Name;
        DeleteProcessedDocuments = queue.DeleteProcessedDocuments;
    }
}
