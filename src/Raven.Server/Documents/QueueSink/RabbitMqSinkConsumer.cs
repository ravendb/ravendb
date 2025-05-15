using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace Raven.Server.Documents.QueueSink;

public class RabbitMqSinkConsumer : AsyncDefaultBasicConsumer, IQueueSinkConsumer
{
    private readonly IChannel _channel;
    private readonly BlockingCollection<(byte[] Body, IReadOnlyBasicProperties Properties, ulong deliveryTag)> _deliveries = new();

    private ulong _latestDeliveryTag;

    public RabbitMqSinkConsumer(IChannel channel) : base(channel)
    {
        _channel = channel;
    }

    public override Task HandleBasicDeliverAsync(string consumerTag, ulong deliveryTag, bool redelivered, string exchange, string routingKey, IReadOnlyBasicProperties properties,
        ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
    {
        _deliveries.Add((body.ToArray(), properties, deliveryTag));
        return Task.CompletedTask;
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
            _channel.BasicAckAsync(_latestDeliveryTag, true).AsTask().GetAwaiter().GetResult();
        }
    }
    
    public void Dispose()
    {
        _channel.Dispose();
    }
}
