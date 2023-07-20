using System;
using System.Collections.Concurrent;
using System.Threading;
using RabbitMQ.Client;

namespace Raven.Server.Documents.QueueSink;

public class QueueSinkRabbitMqConsumer : DefaultBasicConsumer
{
    private readonly BlockingCollection<(byte[] Body, IBasicProperties Properties, ulong deliveryTag)> _deliveries = new();

    public QueueSinkRabbitMqConsumer(IModel model) : base(model)
    {
    }

    public override void HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange,
        string routingKey, IBasicProperties properties, ReadOnlyMemory<byte> body)
    {
        _deliveries.Add((body.ToArray(), properties, deliveryTag));
    }

    public (byte[] Body, IBasicProperties Properties, ulong DeliveryTag) Consume(CancellationToken cancellationToken)
    {
        _deliveries.TryTake(out var delivery, Timeout.Infinite, cancellationToken);
        return delivery;
    }
    
    public (byte[] Body, IBasicProperties Properties, ulong DeliveryTag) Consume(TimeSpan timeout)
    {
        _deliveries.TryTake(out var delivery, timeout.Milliseconds);
        return delivery;
    }
    
    // todo: dispose
}
