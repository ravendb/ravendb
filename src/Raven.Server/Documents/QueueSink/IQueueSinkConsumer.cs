using System.Threading;
using System;

namespace Raven.Server.Documents.QueueSink;

public interface IQueueSinkConsumer : IDisposable
{
    public byte[] Consume(CancellationToken cancellationToken);

    public byte[] Consume(TimeSpan timeout);

    public void Commit();
}
