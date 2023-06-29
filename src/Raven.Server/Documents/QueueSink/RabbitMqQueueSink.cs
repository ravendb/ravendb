using System.Collections.Generic;
using System.Threading;
using Raven.Client.Documents.Operations.QueueSink;

namespace Raven.Server.Documents.QueueSink;

public class RabbitMqQueueSink : QueueSinkProcess
{
    public RabbitMqQueueSink(QueueSinkConfiguration configuration, QueueSinkScript script, DocumentDatabase database, string resourceName, CancellationToken shutdown) : base(configuration, script, database, resourceName, shutdown)
    {
    }

    protected override List<byte[]> ConsumeMessages()
    {
        throw new System.NotImplementedException();
    }

    protected override void Commit()
    {
        throw new System.NotImplementedException();
    }
}
