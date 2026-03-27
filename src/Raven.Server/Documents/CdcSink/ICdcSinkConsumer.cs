using System;
using System.Threading;

namespace Raven.Server.Documents.CdcSink;

public interface ICdcSinkConsumer : IDisposable
{
    public byte[] Consume(CancellationToken cancellationToken);

    public byte[] Consume(TimeSpan timeout);

    public void Commit();
}
