using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Exceptions.ETL.QueueEtl;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents.ETL.Providers.Queue.AmazonSqs;

public sealed class AmazonSqsEtl : QueueEtl<AmazonSqsItem>
{
    private const string FifoQueueIdentifier = ".fifo";
    private static readonly Regex NonAlphanumericRegex = new Regex("[^a-zA-Z0-9]", RegexOptions.Compiled);
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
            bool isFifoQueue = queueName.EndsWith(FifoQueueIdentifier);

            if (_queueClient == null)
            {
                _queueClient = QueueBrokerConnectionHelper.CreateAmazonSqsClient(
                    Configuration.Connection.AmazonSqsConnectionSettings);
            }

            if (Configuration.SkipAutomaticQueueDeclaration == false &&
                _alreadyCreatedQueues.ContainsKey(queueName) == false)
            {
                AsyncHelpers.RunSync(() => CreateQueue(_queueClient, queueName, isFifoQueue));
            }

            var batchMessages = new List<SendMessageBatchRequestEntry>();

            foreach (AmazonSqsItem queueItem in queue.Items)
            {
                CancellationToken.ThrowIfCancellationRequested();

                try
                {
                    string message = SerializeCloudEvent(queueItem, out string messageGroupId);
                    string messageId = CreateAmazonBatchMessageId(queueItem.DocumentId); 
                    
                    var sendMessageEntry = new SendMessageBatchRequestEntry
                    {
                        Id = messageId,
                        MessageBody = message
                    };

                    if (isFifoQueue)
                    {
                        sendMessageEntry.MessageDeduplicationId = messageId;
                        sendMessageEntry.MessageGroupId = messageGroupId;
                    }

                    batchMessages.Add(sendMessageEntry);

                    if (batchMessages.Count == 10)
                    {
                        ProcessBatchMessages(queueName, batchMessages, queue, ref count, ref idsToDelete,
                            ref tooLargeDocsErrors);
                    }
                }
                catch (Exception ex)
                {
                    throw new QueueLoadException($"Failed to deliver message, error reason: '{ex.Message}'", ex);
                }
            }

            // handle remaining messages in batch
            if (batchMessages.Count > 0)
            {
                ProcessBatchMessages(queueName, batchMessages, queue, ref count, ref idsToDelete,
                    ref tooLargeDocsErrors);
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
    
    private void ProcessBatchMessages(string queueName,
        List<SendMessageBatchRequestEntry> batchMessages,
        QueueWithItems<AmazonSqsItem> queue, ref int count, ref List<string> idsToDelete,
        ref Queue<EtlErrorInfo> tooLargeDocsErrors)
    {
        if (TrySendBatchMessages(queueName, batchMessages) == false)
        {
            // If batch sending failed, send each message individually
            SendMessagesOneByOne(queueName, batchMessages, queue, ref idsToDelete, ref tooLargeDocsErrors);
        }
        else
        {
            count += batchMessages.Count;
            if (queue.DeleteProcessedDocuments)
            {
                foreach (var entry in batchMessages)
                {
                    var originalItem = queue.Items.FirstOrDefault(item => item.ChangeVector == entry.Id);
                    if (originalItem != null)
                    {
                        idsToDelete.Add(originalItem.DocumentId);
                    }
                }
            }
        }

        batchMessages.Clear();
    }


    private bool TrySendBatchMessages(string queueName, List<SendMessageBatchRequestEntry> batchMessages)
    {
        try
        {
            var sendMessageBatchRequest = new SendMessageBatchRequest
            {
                QueueUrl = GetQueueUrl(_queueClient, queueName),
                Entries = batchMessages
            };

            AsyncHelpers.RunSync(() => _queueClient.SendMessageBatchAsync(sendMessageBatchRequest));
            return true;
        }
        catch (Exception ex)
        {
            if (Logger.IsWarnEnabled)
                Logger.Warn($"ETL process: {Name}. Failed to send messages in a batch.", ex);
            return false;
        }
    }

    private void SendMessagesOneByOne(string queueName, List<SendMessageBatchRequestEntry> batchMessages,
        QueueWithItems<AmazonSqsItem> queue, ref List<string> idsToDelete, ref Queue<EtlErrorInfo> tooLargeDocsErrors)
    {
        foreach (var message in batchMessages)
        {
            try
            {
                var sendMessageRequest = new SendMessageRequest
                {
                    MessageGroupId = message.MessageGroupId,
                    QueueUrl = GetQueueUrl(_queueClient, queueName),
                    MessageBody = message.MessageBody
                };

                AsyncHelpers.RunSync(() => _queueClient.SendMessageAsync(sendMessageRequest));

                if (queue.DeleteProcessedDocuments)
                {
                    var originalItem = queue.Items.FirstOrDefault(item => item.ChangeVector == message.Id);
                    if (originalItem != null)
                    {
                        idsToDelete.Add(originalItem.DocumentId);
                    }
                }
            }
            catch (AmazonSQSException sqsEx)
            {
                if (sqsEx.ErrorCode == "InvalidAttributeValue")
                {
                    tooLargeDocsErrors.Enqueue(new EtlErrorInfo()
                    {
                        Date = DateTime.UtcNow,
                        DocumentId = message.Id,
                        Error = sqsEx.Message
                    });
                }
                else
                {
                    throw new QueueLoadException(
                        $"Failed to deliver message, Amazon error code: '{sqsEx.ErrorCode}', error reason: '{sqsEx.Message}' for document with id: '{message.Id}'",
                        sqsEx);
                }
            }
            catch (Exception ex)
            {
                throw new QueueLoadException($"Failed to deliver message, error reason: '{ex.Message}'", ex);
            }
        }
    }


    private string SerializeCloudEvent(AmazonSqsItem queueItem, out string messageGroupId)
    {
        var cloudEvent = CreateCloudEvent(queueItem);
        messageGroupId = cloudEvent.Type;
        return JsonSerializer.Serialize(cloudEvent, JsonSerializerOptions);
    }
    
    private static string CreateAmazonBatchMessageId(string documentId)
    {
        string formattedString = NonAlphanumericRegex.Replace(documentId, "-");

        if (formattedString.Length > 80)
        {
            formattedString = formattedString.Substring(0, 70) + "-" +
                              $"{(Hashing.XXHash64.Calculate(formattedString, Encoding.UTF8) % 1_000_000_000)}";
            
        }

        return formattedString;
    }

    protected override void OnProcessStopped()
    {
        _queueClient?.Dispose();
        _queueClient = null;
        _alreadyCreatedQueues.Clear();
    }

    private async Task CreateQueue(IAmazonSQS queueClient, string queueName, bool isFifoQueue)
    {
        try
        {
            CreateQueueResponse createQueueResponse;
            
            if (isFifoQueue)
            {
                createQueueResponse = await queueClient.CreateQueueAsync(new CreateQueueRequest()
                {
                    Attributes = new Dictionary<string, string>()
                    {
                        { "FifoQueue", "true" }
                    },
                    QueueName = queueName,
                });
            }
            else
            {
                createQueueResponse = await queueClient.CreateQueueAsync(queueName);    
            }
            
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
    
    private string GetQueueUrl(IAmazonSQS queueClient, string queueName)
    {
        try
        {
            string queueUrl = _alreadyCreatedQueues.GetValueOrDefault(queueName);
            
            if (string.IsNullOrEmpty(queueUrl))
            {
                GetQueueUrlResponse getQueueUrlResponse = AsyncHelpers.RunSync(() => queueClient.GetQueueUrlAsync(queueName));
                _alreadyCreatedQueues.Add(queueName, getQueueUrlResponse.QueueUrl);
                queueUrl = getQueueUrlResponse.QueueUrl;
            }

            return queueUrl;
        }
        catch (QueueDoesNotExistException ex)
        {
            throw new QueueLoadException(
                $"Queue does not exist, Aws error code: '{ex.ErrorCode}', error reason: '{ex.Message}'", ex);
        }
        catch (AmazonSQSException ex)
        {
            throw new QueueLoadException(
                $"Failed to retrieve the queue, Aws error code: '{ex.ErrorCode}', error reason: '{ex.Message}'", ex);
        }
    }
}
