using System;
using System.Collections.Generic;
using System.Threading;
using RabbitMQ.Client;
using Raven.Client.Documents.Operations.QueueSink;

namespace Raven.Server.Documents.QueueSink;

public class RabbitMqQueueSink : QueueSinkProcess
{
    public RabbitMqQueueSink(QueueSinkConfiguration configuration, QueueSinkScript script, DocumentDatabase database, string resourceName, CancellationToken shutdown) : base(configuration, script, database, resourceName, shutdown)
    {
    }

    private IModel _channel;
    private List<ulong> messagesTags = new();

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
                    messagesTags.Add(message.DeliveryTag);
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
        
        //var message = channel.BasicGet("my_queue", autoAck: false);
        /*var consumer = new EventingBasicConsumer(channel);
        consumer.Received += (model, ea) =>
        {
            var body = ea.Body.ToArray();
            result.Add(body);
        }

        foreach (string queue in Script.Queues)
        {
            channel.BasicConsume(queue: queue, autoAck: true, consumer: consumer);    
        };*/
        
        return messageBatch;
    }

    protected override void Commit()
    {
        foreach (var tag in messagesTags)
        {
            _channel.BasicAck(tag, false);
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
