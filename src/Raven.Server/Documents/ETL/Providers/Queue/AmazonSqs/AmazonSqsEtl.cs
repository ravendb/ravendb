using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Extensions;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Exceptions.ETL.QueueEtl;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.Queue.AmazonSqs;

public sealed class AmazonSqsEtl : QueueEtl<AmazonSqsItem>
{
    private readonly Dictionary<string, string> _alreadyCreatedQueues = new();
    private IAmazonSQS _queueClient;

    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        Converters = { CloudEventConverter.Instance }
    };

    public AmazonSqsEtl(Transformation transformation, QueueEtlConfiguration configuration,
        DocumentDatabase database, ServerStore serverStore) : base(transformation, configuration, database, serverStore)
    {
    }

    protected override
        EtlTransformer<QueueItem, QueueWithItems<AmazonSqsItem>, EtlStatsScope, EtlPerformanceOperation>
        GetTransformer(DocumentsOperationContext context)
    {
        return new AmazonSqsDocumentTransformer<AmazonSqsItem>(Transformation, Database, context,
            Configuration);
    }

    protected override int PublishMessages(List<QueueWithItems<AmazonSqsItem>> itemsPerQueue,
        BlittableJsonEventBinaryFormatter formatter, out List<string> idsToDelete)
    {
        if (itemsPerQueue.Count == 0)
        {
            idsToDelete = null;
            return 0;
        }

        var tooLargeDocsErrors = new Queue<EtlErrorInfo>();
        idsToDelete = new List<string>();
        int count = 0;

        foreach (QueueWithItems<AmazonSqsItem> queue in itemsPerQueue)
        {
            string queueName = queue.Name.ToLower();

            if (_queueClient == null)
            {
                _queueClient = QueueBrokerConnectionHelper.CreateAmazonSqsClient(
                    Configuration.Connection.AmazonSqsConnectionSettings);
            }

            if (Configuration.SkipAutomaticQueueDeclaration == false &&
                _alreadyCreatedQueues.ContainsKey(queueName) == false)
                AsyncHelpers.RunSync(() => CreateQueue(_queueClient, queueName));

            var batchMessages = new List<SendMessageBatchRequestEntry>();

            foreach (AmazonSqsItem queueItem in queue.Items)
            {
                CancellationToken.ThrowIfCancellationRequested();

                try
                {
                    var sendMessageEntry = new SendMessageBatchRequestEntry
                    {
                        Id = queueItem.DocumentId,
                        MessageBody = SerializeCloudEvent(queueItem)
                    };

                    batchMessages.Add(sendMessageEntry);

                    if (batchMessages.Count == 10)
                    {
                        SendBatchMessages(queueName, batchMessages, queue, idsToDelete, tooLargeDocsErrors);
                        count += batchMessages.Count;
                        batchMessages.Clear();
                    }
                }
                catch (Exception ex)
                {
                    throw new QueueLoadException($"Failed to prepare message, error reason: '{ex.Message}'", ex);
                }
            }

            if (batchMessages.Count > 0)
            {
                SendBatchMessages(queueName, batchMessages, queue, idsToDelete, tooLargeDocsErrors);
                count += batchMessages.Count;
                batchMessages.Clear();
            }

            if (tooLargeDocsErrors.Count > 0)
            {
                Database.NotificationCenter.EtlNotifications.AddLoadErrors(Tag, Name, tooLargeDocsErrors,
                    "ETL has partially loaded the data. " +
                    "Some of the documents were too big (>256KB) to be handled by Amazon SQS. " +
                    "It caused load errors, that have been skipped. ");
            }
        }

        return count;
    }

    private void SendBatchMessages(string queueName, List<SendMessageBatchRequestEntry> batchMessages,
        QueueWithItems<AmazonSqsItem> queue, List<string> idsToDelete, Queue<EtlErrorInfo> tooLargeDocsErrors)
    {
        try
        {
            var sendMessageBatchRequest = new SendMessageBatchRequest
            {
                QueueUrl = _alreadyCreatedQueues.GetValueOrDefault(queueName),
                Entries = batchMessages
            };

            AsyncHelpers.RunSync(() => _queueClient.SendMessageBatchAsync(sendMessageBatchRequest));

            if (queue.DeleteProcessedDocuments)
            {
                idsToDelete.AddRange(batchMessages.Select(entry => entry.Id));
            }
        }
        catch (Exception)
        {
            // Retry sending one by one if batch fails
            foreach (var message in batchMessages)
            {
                try
                {
                    var sendMessageRequest = new SendMessageRequest
                    {
                        QueueUrl = _alreadyCreatedQueues.GetValueOrDefault(queueName),
                        MessageBody = message.MessageBody
                    };

                    AsyncHelpers.RunSync(() => _queueClient.SendMessageAsync(sendMessageRequest));

                    if (queue.DeleteProcessedDocuments)
                    {
                        idsToDelete.Add(message.Id);
                    }
                }
                catch (AmazonSQSException sqsEx)
                {
                    if (sqsEx.ErrorCode == "InvalidAttributeValue")
                    {
                        tooLargeDocsErrors.Enqueue(new EtlErrorInfo()
                        {
                            Date = DateTime.UtcNow, DocumentId = message.Id, Error = sqsEx.Message
                        });
                    }
                    else
                    {
                        throw new QueueLoadException(
                            $"Failed to deliver message, Aws error code: '{sqsEx.ErrorCode}', error reason: '{sqsEx.Message}' for document with id: '{message.Id}'",
                            sqsEx);
                    }
                }
                catch (Exception innerEx)
                {
                    throw new QueueLoadException($"Failed to deliver message, error reason: '{innerEx.Message}'",
                        innerEx);
                }
            }
        }
    }


    private string SerializeCloudEvent(AmazonSqsItem queueItem)
    {
        var cloudEvent = CreateCloudEvent(queueItem);
        return JsonSerializer.Serialize(cloudEvent, JsonSerializerOptions);
    }

    protected override void OnProcessStopped()
    {
        _queueClient?.Dispose();
        _queueClient = null;
        _alreadyCreatedQueues.Clear();
    }

    private async Task CreateQueue(IAmazonSQS queueClient, string queueName)
    {
        try
        {
            CreateQueueResponse createQueueResponse = await queueClient.CreateQueueAsync(queueName);
            _alreadyCreatedQueues.Add(queueName, createQueueResponse.QueueUrl);

            // we must wait at least one second after the queue is created to be able to use the queue
            await Task.Delay(TimeSpan.FromMilliseconds(1000));
        }
        catch (AmazonSQSException ex)
        {
            throw new QueueLoadException(
                $"Failed to create queue, Aws error code: '{ex.ErrorCode}', error reason: '{ex.Message}'", ex);
        }
    }
}
