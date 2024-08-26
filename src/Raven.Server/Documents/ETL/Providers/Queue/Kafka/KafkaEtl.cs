using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.Kafka;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Util;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Exceptions.ETL.QueueEtl;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using ResourceType = Confluent.Kafka.Admin.ResourceType;

namespace Raven.Server.Documents.ETL.Providers.Queue.Kafka;

public sealed class KafkaEtl : QueueEtl<KafkaItem>
{
    private IProducer<string, byte[]> _producer;
    internal string TransactionalId { get; }

    public KafkaEtl(Transformation transformation, QueueEtlConfiguration configuration, DocumentDatabase database, ServerStore serverStore) : base(transformation, configuration, database, serverStore)
    {
        TransactionalId = EnsureValidTransactionalId($"{Name}-{Database.DatabaseGroupId}");
    }


    protected override EtlTransformer<QueueItem, QueueWithItems<KafkaItem>, EtlStatsScope, EtlPerformanceOperation> GetTransformer(DocumentsOperationContext context)
    {
        return new KafkaDocumentTransformer<KafkaItem>(Transformation, Database, context, Configuration);
    }

    private static string EnsureValidTransactionalId(string transactionalId)
    {
        return transactionalId.Replace("/", "_");
    }

    public override EtlProcessProgress GetProgress(DocumentsOperationContext documentsContext)
    {
        var result = base.GetProgress(documentsContext);
        result.TransactionalId = TransactionalId;
        return result;
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
            catch (KafkaException e)
            {
                producer.Dispose();
                var errorMessageBuilder = new StringBuilder($"ETL process: {Name}. Failed to initialize transactions for the producer instance. Error code: '{e.Error.Code}'. Error reason: '{e.Error.Reason}'{Environment.NewLine}");
                if (e.Error.Code == ErrorCode.TransactionalIdAuthorizationFailed)
                {
                    errorMessageBuilder.Append(
                        $"Add an required ACL permissions to your API Key that will make WRITE and DESCRIBE operations possible for transactional ID '{TransactionalId}'. ");
                }
                else
                {
                    errorMessageBuilder.Append($"If you are using a single node Kafka cluster then the following settings might be required:{Environment.NewLine}" +
                                               $"- transaction.state.log.replication.factor: 1 {Environment.NewLine}" +
                                               "- transaction.state.log.min.isr: 1");
                }

                throw new QueueLoadException(errorMessageBuilder.ToString(), e);
            }
            catch (Exception e)
            {
                producer.Dispose();
                throw new QueueLoadException("Initialize transactions failed.", e);
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
                if (Logger.IsErrorEnabled)
                    Logger.Error($"Failed to deliver message '{report.Key}', Kafka error code: '{report.Error.Code}', error reason: '{report.Error.Reason}'");
            }
        }
        if (itemsPerTopic.Count == 0)
            return count;
        
        
        try
        {
            _producer.BeginTransaction();
        }
        catch (KafkaException e)
        {
            var errorMessageBuilder = new StringBuilder($"ETL process: {Name}. Begin transaction failed. Kafka error code: '{e.Error.Code}'. Error reason: '{e.Error.Reason}'");
            if (TryGetMissingAclsBindings(out var detailsMessage)) // Check if we still have valid transactional id ACLs set
                errorMessageBuilder.Append(detailsMessage);
            throw new QueueLoadException(errorMessageBuilder.ToString(), e);
        }
        catch (Exception e)
        {
            throw new QueueLoadException($"ETL process: {Name}. Begin transaction failed.", e);
        }

        try
        {
            foreach (var topic in itemsPerTopic)
            {
                foreach (var queueItem in topic.Items)
                {
                    CancellationToken.ThrowIfCancellationRequested();

                    var cloudEvent = CreateCloudEvent(queueItem);

                    var kafkaMessage = cloudEvent.ToKafkaMessage(ContentMode.Binary, formatter);

                    _producer.Produce(topic.Name, kafkaMessage, ReportHandler);

                    if (topic.DeleteProcessedDocuments)
                        idsToDelete.Add(queueItem.DocumentId);
                }
            }
        }
        catch (KafkaException e)
        {
            var errorMessageBuilder = new StringBuilder($"ETL process: {Name}. Failed to produce message. Kafka error code: '{e.Error.Code}'. Error reason: '{e.Error.Reason}'. {Environment.NewLine}");

            if (e is ProduceException<string, byte[]> produceException)
            {
                if (e.Error.Code == ErrorCode.TopicAuthorizationFailed)
                    errorMessageBuilder.Append($"Add an required ACL permissions to your API Key that will make WRITE operations on the '{produceException.DeliveryResult.Topic}' topic possible.");
                
                else if (TryGetMissingAclsBindings(out var message))
                    errorMessageBuilder.Append(message);
            }
            
            if (TryAbortTransaction(out var abortException))
                throw new QueueLoadException(errorMessageBuilder.ToString(), e);
               
            errorMessageBuilder.Append("Aborting Kafka transaction failed too.");
            throw new QueueLoadException(errorMessageBuilder.ToString(), new AggregateException(e, abortException));
        }
        catch (Exception ex)
        {
            var errorMessageBuilder = new StringBuilder($"ETL process: {Name}. Failed to produce message.{Environment.NewLine}");
            if (TryAbortTransaction(out var abortException))
                throw new QueueLoadException(errorMessageBuilder.ToString(), ex);
            
            errorMessageBuilder.Append("Aborting Kafka transaction failed too.");
            throw new QueueLoadException(errorMessageBuilder.ToString(), new AggregateException(ex, abortException));
        }

        try
        {
            _producer.CommitTransaction();
        }
        catch (KafkaException ex)
        {
            var errorMessageBuilder = new StringBuilder($"ETL process: {Name}. Commit transaction failed. " +
                                                        $"Kafka error code: '{ex.Error.Code}'. Error reason: '{ex.Error.Reason}'{Environment.NewLine}");

            if (ex is not KafkaTxnRequiresAbortException)
            {
                if (TryGetMissingAclsBindings(out var detailsMessage))
                    errorMessageBuilder.Append(detailsMessage);

                throw new QueueLoadException(errorMessageBuilder.ToString(), ex);
            }

            if (TryAbortTransaction(out var abortException))
            {
                errorMessageBuilder.Append("Although transaction is lost, required transaction abort was handled successfully.");
                throw new QueueLoadException(errorMessageBuilder.ToString(), ex);
            }

            errorMessageBuilder.Append("Aborting Kafka transaction failed too.");
            throw new QueueLoadException(errorMessageBuilder.ToString(), new AggregateException(ex, abortException));
        }
        catch (Exception ex)
        {
            throw new QueueLoadException($"ETL process: {Name}. Commit transaction failed.", ex);
        }

        return count;
    }

    private IAdminClient GetAdminClient()
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = Configuration.Connection.KafkaConnectionSettings.BootstrapServers,
        };
        QueueBrokerConnectionHelper.SetupKafkaClientConfig(config, Configuration.Connection.KafkaConnectionSettings);
        return new AdminClientBuilder(config).Build();
    }


    private static IEnumerable<AclBinding> GetMatchingAllowAclBindings(IAdminClient adminClient, AclOperation operation = AclOperation.All,
        ResourceType resourceType = ResourceType.Any)
    {
        var aclBindingsForTransactionIdsTask = adminClient.DescribeAclsAsync(new AclBindingFilter
        {
            EntryFilter = new AccessControlEntryFilter {Operation = operation, PermissionType = AclPermissionType.Allow},
            PatternFilter = new ResourcePatternFilter {ResourcePatternType = ResourcePatternType.Any, Type = resourceType}
        });

        return AsyncHelpers.RunSync(() => aclBindingsForTransactionIdsTask).AclBindings;
    }

    private bool TryGetMissingAclsBindings(out string message, string topicName = null)
    {
        message = "";
        try
        {
            using var adminClient = GetAdminClient();

            // unknown resource type is assigned to returned transactionalID ACL bindings, but we cannot directly ask API for unknown type resources, so we have to filter.. 
            var transactionIdsWriteAclBindings = GetMatchingAllowAclBindings(adminClient, AclOperation.Write).Where(x => x.Pattern.Type == ResourceType.Unknown);
            var transactionIdsDescribeAclBindings = GetMatchingAllowAclBindings(adminClient, AclOperation.Describe).Where(x => x.Pattern.Type == ResourceType.Unknown);
            
            bool validTransactionWriteAcl =
                transactionIdsWriteAclBindings.Any(transactionIdsAclBinding => AssertAclMatchingName(TransactionalId, transactionIdsAclBinding));
            bool validTransactionDescribeAcl =
                transactionIdsDescribeAclBindings.Any(transactionIdsAclBinding => AssertAclMatchingName(TransactionalId, transactionIdsAclBinding));
            
            
            bool validTopicWriteAcl = true;
            if (topicName is not null)
            {
                var topicsWriteAclBindings = GetMatchingAllowAclBindings(adminClient, AclOperation.Write, ResourceType.Topic);
                validTopicWriteAcl = topicsWriteAclBindings.Any(topicsAclBinding => AssertAclMatchingName(topicName, topicsAclBinding));    
            }
            
            if (validTransactionWriteAcl && validTopicWriteAcl && validTransactionDescribeAcl)
            {
                return false;
            }

            var msgBuilder = new StringBuilder($"Please check your Kafka security configuration " +
                                               $"and ensure all of the (RESOURCE : OPERATION) ACL bindings listed below are set to ALLOW: {Environment.NewLine}");

            if ((validTransactionDescribeAcl && validTransactionWriteAcl) == false)
            {
                msgBuilder.Append($"Transactional ID '{TransactionalId}' : ");
                if (validTransactionWriteAcl == false)
                    msgBuilder.Append("WRITE operation ");
                if (validTransactionDescribeAcl == false)
                    msgBuilder.Append("DESCRIBE operation");
                msgBuilder.Append(Environment.NewLine);
            }

            
            if (validTopicWriteAcl == false)
                msgBuilder.Append($"Topic '{topicName}' : WRITE operation");

            message = msgBuilder.ToString();

        }
        catch (Exception)
        {
            return false;
        }

        return true;
    }

    private static bool AssertAclMatchingName(string expectedName, AclBinding aclBinding)
    {
        return aclBinding.Pattern.ResourcePatternType switch
        {
            ResourcePatternType.Literal => IsMatchingLiterally(aclBinding.Pattern.Name, expectedName),
            ResourcePatternType.Prefixed => IsPrefixMatching(aclBinding.Pattern.Name, expectedName),
            ResourcePatternType.Match => Regex.IsMatch(aclBinding.Pattern.Name, expectedName),
            _ => throw new NotSupportedException(
                $"Resource pattern type '{aclBinding.Pattern.ResourcePatternType}' is not supported.{Environment.NewLine}" +
                $"Supported resource types are: '{ResourcePatternType.Literal}', '{ResourcePatternType.Prefixed}, and {ResourcePatternType.Match}.")
        };
    }
    
    private static bool IsMatchingLiterally(string pattern, string expected)
    {
        if (pattern.Contains('*') == false) 
            return pattern == expected;
        
        string patternRegex = "^" + Regex.Escape(pattern).Replace("\\*", ".*") + "$";
        return Regex.IsMatch(expected, patternRegex);
    }
    
    private static bool IsPrefixMatching(string pattern, string expected)
    {
        if (pattern.Contains('*') == false)
            return expected.StartsWith(pattern);
        
        // If the pattern contains '*', use wildcard matching in the prefix
        string[] parts = pattern.Split('*');
        
        // if pattern is '*', it's a match
        if (parts.Length == 0) 
            return true; 
        
        int index = 0; // try to find part after part in the expected string
        foreach (var substring in parts)
        {
            index = expected.IndexOf(substring, index, StringComparison.Ordinal);
            if (index == -1) 
                return false; // If a part is not found in order, it's not a match
            index += substring.Length;
        }
        return true;
    }
    

    private bool TryAbortTransaction(out Exception abortException)
    {
        abortException = null;
        try
        {
            _producer.AbortTransaction();
        }
        catch (Exception ex)
        {
            abortException = ex;
            return false;
        }

        return true;
    }

    protected override void OnProcessStopped()
    {
        _producer?.Dispose();
        _producer = null;
    }
}
