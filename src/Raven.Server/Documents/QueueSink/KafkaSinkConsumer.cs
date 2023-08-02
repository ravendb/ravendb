using System;
using System.Threading;
using Confluent.Kafka;

namespace Raven.Server.Documents.QueueSink;

public class KafkaSinkConsumer : IQueueSinkConsumer
{
    private readonly IConsumer<string, byte[]> _consumer;

    public KafkaSinkConsumer(IConsumer<string, byte[]> consumer)
    {
        _consumer = consumer;
    }

    public byte[] Consume(CancellationToken cancellationToken)
    {
        var result = _consumer.Consume(cancellationToken);

        return result?.Message?.Value;
    }

    public byte[] Consume(TimeSpan timeout)
    {
        var result = _consumer.Consume(TimeSpan.Zero);

        return result?.Message?.Value;
    }

    public void Commit()
    {
        _consumer.Commit();
    }

    public void Dispose()
    {
        _consumer.Dispose();
    }
}
