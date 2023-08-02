using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Server.Documents.QueueSink.Stats;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.QueueSink;

public class RabbitMqQueueSink : QueueSinkProcess
{
    public RabbitMqQueueSink(QueueSinkConfiguration configuration, QueueSinkScript script, DocumentDatabase database,
        string resourceName, CancellationToken shutdown) : base(configuration, script, database, resourceName, shutdown)
    {
    }

    private IModel _channel;
    private QueueSinkRabbitMqConsumer _consumer;
    private ulong? _latestDeliveryTag = new();

    protected override async Task<List<BlittableJsonReaderObject>> ConsumeMessages(DocumentsOperationContext context, QueueSinkStatsScope stats)
    {
        var messageBatch = new List<BlittableJsonReaderObject>();

        if (_channel == null || _channel.IsClosed)
        {
            try
            {
                _channel = CreateRabbitMqChannel();
                _consumer = new QueueSinkRabbitMqConsumer(_channel);
                foreach (string queue in Script.Queues)
                {
                    _channel.BasicConsume(queue: queue, autoAck: false, consumer: _consumer);
                }
            }
            catch (Exception e)
            {
                string msg = $"Failed to create rabbitmq channel for {Name}.";

                if (Logger.IsOperationsEnabled)
                {
                    Logger.Operations(msg, e);
                }

                EnterFallbackMode();

                return messageBatch;
            }
        }

        while (messageBatch.Count < Database.Configuration.QueueSink.MaxNumberOfConsumedMessagesInBatch)
        {
            try
            {
                var message = messageBatch.Count == 0
                    ? _consumer.Consume(CancellationToken)
                    : _consumer.Consume(TimeSpan.Zero);
                
                if (message.Body is null)
                    break;

                if (stats.IsRunning == false)
                    stats.Start();

                var jsonMessage = await context.ReadForMemoryAsync(new MemoryStream(message.Body), "queue-message", CancellationToken);

                messageBatch.Add(jsonMessage);
                _latestDeliveryTag = message.DeliveryTag > _latestDeliveryTag
                    ? message.DeliveryTag
                    : _latestDeliveryTag;

                stats.RecordConsumedMessage();
            }
            catch (OperationCanceledException) when (CancellationToken.IsCancellationRequested)
            {
                return messageBatch;
            }
            catch (Exception e)
            {
                string msg = $"Failed to consume message.";
                if (Logger.IsOperationsEnabled)
                {
                    Logger.Operations(msg, e);
                }

                EnterFallbackMode();
                Statistics.RecordConsumeError(e.Message, 0);
                return messageBatch;
            }
        }
        
        return messageBatch;
    }

    protected override void Commit()
    {
        if (_latestDeliveryTag.HasValue)
        {
            _channel.BasicAck(_latestDeliveryTag.Value, true);
        }
    }

    private IModel CreateRabbitMqChannel()
    {
        var connectionFactory = new ConnectionFactory { Uri = new Uri(Configuration.Connection.RabbitMqConnectionSettings.ConnectionString) };
        var connection = connectionFactory.CreateConnection();
        var channel = connection.CreateModel();

        return channel;
    }
}
