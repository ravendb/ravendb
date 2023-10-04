using System;
using System.Collections.Generic;
using System.Diagnostics;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Kafka;
using Confluent.Kafka;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Exceptions.ETL.QueueEtl;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.Queue.Kafka;

public class KafkaEtl : QueueEtl<KafkaItem>
{
    private IProducer<string, byte[]> _producer;

    public KafkaEtl(Transformation transformation, QueueEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore) : base(transformation, configuration, database, serverStore)
    {
    }

    private string TransactionalId => $"{Database.DbId}-{Name}";

    protected override EtlTransformer<QueueItem, QueueWithItems<KafkaItem>, EtlStatsScope, EtlPerformanceOperation> GetTransformer(DocumentsOperationContext context)
    {
        return new KafkaDocumentTransformer<KafkaItem>(Transformation, Database, context, Configuration);
    }

    protected override int PublishMessages(List<QueueWithItems<KafkaItem>> itemsPerTopic, BlittableJsonEventBinaryFormatter formatter, out List<string> idsToDelete)
    {
        if (itemsPerTopic.Count == 0)
        {
            idsToDelete = null;
            return 0;
        }

        idsToDelete = new List<string>();

        int count = 0;

        if (_producer == null)
        {
            var producer = QueueBrokerConnectionHelper.CreateKafkaProducer(Configuration.Connection.KafkaConnectionSettings, TransactionalId, Logger, Name,
                Database.ServerStore.Server.Certificate);

            try
            {
                var sw = Stopwatch.StartNew();

                do
                {
                    try
                    {
                        producer.InitTransactions(TimeSpan.FromSeconds(10)); // let's wait up to 10 second so we can check if cancellation was requested meanwhile
                        break;
                    }
                    catch (KafkaRetriableException e)
                    {
                        if (sw.Elapsed < Database.Configuration.Etl.KafkaInitTransactionsTimeout.AsTimeSpan)
                        {
                            // let it retry up to configured timeout

                            if (Logger.IsInfoEnabled)
                                Logger.Info($"ETL process: {Name}. Failed to init transactions for the producer instance. Already waited: {sw.Elapsed}", e);
                        }
                        else
                        {
                            throw;
                        }
                    }

                    CancellationToken.ThrowIfCancellationRequested();

                } while (true);
            }
            catch (Exception e)
            {
                producer.Dispose();

                string msg = $" ETL process: {Name}. Failed to initialize transactions for the producer instance. " +
                             $"If you are using a single node Kafka cluster then the following settings might be required:{Environment.NewLine}" +
                             $"- transaction.state.log.replication.factor: 1 {Environment.NewLine}" +
                             "- transaction.state.log.min.isr: 1";

                if (Logger.IsOperationsEnabled) 
                    Logger.Operations(msg, e);

                throw new QueueLoadException(msg, e);
            }

            _producer = producer;
        }

        void ReportHandler(DeliveryReport<string, byte[]> report)
        {
            if (report.Error.IsError == false)
            {
                count++;
            }
            else
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Failed to deliver message: {report.Error.Reason}");

                try
                {
                    _producer.AbortTransaction();
                }
                catch (Exception e)
                {
                    if (Logger.IsOperationsEnabled)
                        Logger.Operations(
                            $"ETL process: {Name}. Aborting Kafka transaction failed after getting deliver report with error.", e);
                }
            }
        }

        _producer.BeginTransaction();

        try
        {
            foreach (var topic in itemsPerTopic)
            {
                foreach (var queueItem in topic.Items)
                {
                    var cloudEvent = CreateCloudEvent(queueItem);

                    var kafkaMessage = cloudEvent.ToKafkaMessage(ContentMode.Binary, formatter);

                    _producer.Produce(topic.Name, kafkaMessage, ReportHandler);

                    if (topic.DeleteProcessedDocuments)
                        idsToDelete.Add(queueItem.DocumentId);
                }
            }

            _producer.CommitTransaction();
        }
        catch (Exception ex)
        {
            try
            {
                _producer.AbortTransaction();
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($" ETL process: {Name}. Aborting Kafka transaction failed.", e);
            }

            throw new QueueLoadException(ex.Message, ex);
        }

        return count;
    }

    protected override void OnProcessStopped()
    {
        _producer?.Dispose();
        _producer = null;
    }
}
