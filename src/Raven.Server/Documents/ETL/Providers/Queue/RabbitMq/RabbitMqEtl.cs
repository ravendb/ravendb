using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Amqp;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Exceptions.ETL.QueueEtl;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue.RabbitMq;

public sealed class RabbitMqEtl : QueueEtl<RabbitMqItem>
{
    public static readonly string DefaultExchange = string.Empty;
    public static readonly string DefaultRoutingKey = string.Empty;

    private readonly HashSet<string> _alreadyCreatedExchanges = new();
    private readonly HashSet<string> _alreadyCreatedQueues = new();

    private IConnection _connection;

    public RabbitMqEtl(Transformation transformation, QueueEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore) : base(transformation, configuration, database, serverStore)
    {
    }

    protected override EtlTransformer<QueueItem, QueueWithItems<RabbitMqItem>, EtlStatsScope, EtlPerformanceOperation> GetTransformer(DocumentsOperationContext context, EtlStatsScope stats)
    {
        return new RabbitMqDocumentTransformer<RabbitMqItem>(Transformation, Database, context, Configuration);
    }

    protected override int PublishMessages(List<QueueWithItems<RabbitMqItem>> itemsPerExchange, BlittableJsonEventBinaryFormatter formatter, out List<string> idsToDelete)
    {
        var result = PublishMessagesAsync(itemsPerExchange, formatter).GetAwaiter().GetResult();
        idsToDelete = result.IdsToDelete;
        return result.Count;
    }

    private async Task<(int Count, List<string> IdsToDelete)> PublishMessagesAsync(List<QueueWithItems<RabbitMqItem>> itemsPerExchange, BlittableJsonEventBinaryFormatter formatter)
    {
        if (itemsPerExchange.Count == 0)
        {
            return (0, null);
        }

        var idsToDelete = new List<string>();

        int count = 0;

        if (_connection == null)
        {
            _connection = QueueBrokerConnectionHelper.CreateRabbitMqConnection(Configuration.Connection.RabbitMqConnectionSettings);
        }

        if (Configuration.SkipAutomaticQueueDeclaration == false)
            await DeclareDefaultExchangesQueuesAndBindingsAsync(itemsPerExchange);

        // withing a single transaction we can publish messages to the same exchange

        foreach (var exchange in itemsPerExchange)
        {
            var exchangeName = exchange.Name;

            using var producer = await _connection.CreateChannelAsync();

            await producer.TxSelectAsync();

            var batch = new List<Task>();

            foreach (var queueItem in exchange.Items)
            {
                CancellationToken.ThrowIfCancellationRequested();

                var properties = new BasicProperties();

                properties.Headers = new Dictionary<string, object>();

                var cloudEvent = CreateCloudEvent(queueItem);

                var rabbitMqMessage = cloudEvent.ToAmqpMessage(ContentMode.Binary, formatter);

                foreach (var appProperty in rabbitMqMessage.ApplicationProperties.Map.ToList())
                {
                    string key = appProperty.Key?.ToString();
                    string value = appProperty.Value?.ToString();

                    if (key != null)
                    {
                        properties.Headers.Add(key, value);
                    }
                }

                batch.Add(producer.BasicPublishAsync(exchangeName ?? DefaultExchange, queueItem.RoutingKey ?? DefaultRoutingKey, true, properties, new ReadOnlyMemory<byte>((byte[])rabbitMqMessage.Body)).AsTask());
                count++;

                if (exchange.DeleteProcessedDocuments)
                    idsToDelete.Add(queueItem.DocumentId);
            }
            
            await Task.WhenAll(batch.ToArray());
            
            try
            {
                await producer.TxCommitAsync();
            }
            catch (Exception ex)
            {
                try
                {
                    await producer.TxRollbackAsync();
                }
                catch (Exception e)
                {
                    if (Logger.IsErrorEnabled)
                        Logger.Error($" ETL process: {Name}. Aborting RabbitMQ transaction failed.", e);
                }
                throw new QueueLoadException(ex.Message, ex);
            }
        }

        return (count, idsToDelete);
    }

    protected override void OnProcessStopped()
    {
        _connection?.Dispose();
        _connection = null;
    }

    private async Task DeclareDefaultExchangesQueuesAndBindingsAsync(List<QueueWithItems<RabbitMqItem>> itemsPerExchange)
    {
        using var rabbitMqModel = await _connection.CreateChannelAsync();

        foreach (var item in itemsPerExchange)
        {
            var exchangeName = item.Name;
            var messages = item.Items;

            try
            {
                if (exchangeName == DefaultExchange)
                {
                    foreach (var message in messages)
                    {
                        if (_alreadyCreatedQueues.Contains(message.RoutingKey) == false)
                        {
                            await rabbitMqModel.QueueDeclareAsync(message.RoutingKey, true, false, false, null);
                            _alreadyCreatedQueues.Add(message.RoutingKey);
                        }
                    }
                }
                else if (_alreadyCreatedExchanges.Contains(exchangeName) == false)
                {
                    // in default configuration we create the exchange with Fanout types
                    // it means that routing keys will be ignored

                    await rabbitMqModel.ExchangeDeclareAsync(exchangeName, ExchangeType.Fanout, true, false, null); 
                    await rabbitMqModel.QueueDeclareAsync(exchangeName, true, false, false, null);
                    await rabbitMqModel.QueueBindAsync(exchangeName, exchangeName, DefaultRoutingKey);

                    _alreadyCreatedExchanges.Add(exchangeName);
                    _alreadyCreatedQueues.Add(exchangeName);
                }
            }
            catch (OperationInterruptedException e)
            {
                if (e.ShutdownReason.ReplyCode == 406)
                {
                    // queue already exists
                    continue;
                }

                throw;
            }
        }
    }
}
