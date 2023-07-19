using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Raven.Client.Documents.Operations.QueueSink;

namespace Raven.Server.Documents.QueueSink;

public class RabbitMqQueueSink : QueueSinkProcess
{
    public RabbitMqQueueSink(QueueSinkConfiguration configuration, QueueSinkScript script, DocumentDatabase database, string resourceName, CancellationToken shutdown) : base(configuration, script, database, resourceName, shutdown)
    {
    }

    private IModel _channel;
    private ulong? _latestDeliveryTag = new();

    protected override List<byte[]> ConsumeMessages()
    {
        if (_channel == null || _channel.IsClosed)
        {
            try
            {
                _channel = CreateRabbitMqChannel();
            }
            catch (Exception e)
            {
                string msg = $"Failed to create rabbitmq channel for {Name}.";

                if (Logger.IsOperationsEnabled)
                {
                    Logger.Operations(msg, e);
                }

                EnterFallbackMode();
            }
        }
        
        var messageBatch = new List<byte[]>();

        foreach (string queue in Script.Queues)
        {
            while (true)
            {
                try
                {
                    var message = _channel.BasicGet(queue, autoAck: false);
                    if (message is null) break;
                    messageBatch.Add(message.Body.ToArray());
                    _latestDeliveryTag = message.DeliveryTag > _latestDeliveryTag // todo: check this if it's ok
                        ? message.DeliveryTag
                        : _latestDeliveryTag;
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
