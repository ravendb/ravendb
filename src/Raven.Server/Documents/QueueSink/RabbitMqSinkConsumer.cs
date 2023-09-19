using System;
using System.Collections.Concurrent;
using System.Threading;
using RabbitMQ.Client;

namespace Raven.Server.Documents.QueueSink;

public class RabbitMqSinkConsumer : DefaultBasicConsumer, IQueueSinkConsumer
{
    private readonly IModel _channel;
    private readonly BlockingCollection<(byte[] Body, IBasicProperties Properties, ulong deliveryTag)> _deliveries = new();

    private ulong _latestDeliveryTag;

    public RabbitMqSinkConsumer(IModel channel) : base(channel)
    {
        _channel = channel;
    }

    public override void HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange,
        string routingKey, IBasicProperties properties, ReadOnlyMemory<byte> body)
    {
        _deliveries.Add((body.ToArray(), properties, deliveryTag));
    }

    public byte[] Consume(CancellationToken cancellationToken)
    {
        _deliveries.TryTake(out var delivery, Timeout.Infinite, cancellationToken);

        UpdateDeliveryTag(delivery.deliveryTag);

        return delivery.Body;
    }
    
    public byte[] Consume(TimeSpan timeout)
    {
        _deliveries.TryTake(out var delivery, timeout.Milliseconds);

        UpdateDeliveryTag(delivery.deliveryTag);

        return delivery.Body;
    }

    private void UpdateDeliveryTag(ulong deliveryTag)
    {
        if (deliveryTag > 0 && deliveryTag > _latestDeliveryTag)
            _latestDeliveryTag = deliveryTag;
    }

    public void Commit()
    {
        if (_latestDeliveryTag > 0)
        {
            _channel.BasicAck(_latestDeliveryTag, true);    
        }
    }
    
    public void Dispose()
    {
        _channel.Dispose();
    }
}
