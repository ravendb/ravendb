using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Queue
{
    public sealed class QueueEtlConfiguration : EtlConfiguration<QueueConnectionString>
    {
        public QueueEtlConfiguration()
        {
            Queues = new List<EtlQueue>();
        }

        public List<EtlQueue> Queues { get; set; }

        public QueueBrokerType BrokerType { get; set; }

        public bool SkipAutomaticQueueDeclaration { get; set; }

        public override bool Validate(out List<string> errors, bool validateName = true, bool validateConnection = true, bool validateIdentifier = true, EtlConfiguration<QueueConnectionString> existingConfiguration = null)
        {
            var validationResult = base.Validate(out errors, validateName, validateConnection, validateIdentifier, existingConfiguration);
            if (Connection != null && BrokerType != Connection.BrokerType)
            {
                errors.Add("Broker type must be the same in the ETL configuration and in Connection string.");
                return false;
            }
            return validationResult;
        }
        
        public override string GetDestination()
        {
            return Connection.GetUrl();
        }

        public override EtlType EtlType => EtlType.Queue;
        
        public override bool UsingEncryptedCommunicationChannel()
        {
            switch (BrokerType)
            {
                case QueueBrokerType.Kafka:
                    if (Connection.KafkaConnectionSettings.ConnectionOptions.TryGetValue("security.protocol", out string protocol))
                    {
                        return protocol.ToLower().Contains("ssl");
                    }
                    break;
                case QueueBrokerType.RabbitMq:
                    return Connection.RabbitMqConnectionSettings.ConnectionString.StartsWith("amqps",
                        StringComparison.OrdinalIgnoreCase);
                case QueueBrokerType.AzureQueueStorage:
                    return Connection.AzureQueueStorageConnectionSettings.GetStorageUrl()
                        .StartsWith("https", StringComparison.OrdinalIgnoreCase);
                case QueueBrokerType.AmazonSqs:
                    return Connection.AmazonSqsConnectionSettings.GetQueueUrl()
                        .StartsWith("https", StringComparison.OrdinalIgnoreCase);
                case QueueBrokerType.AzureServiceBus:
                    // Azure Service Bus always uses TLS (AMQP over TLS or AMQP over WebSockets/TLS)
                    return true;
                default:
                    throw new NotSupportedException($"Unknown broker type: {BrokerType}");
            }

            return false;
        }

        public override string GetDefaultTaskName()
        {
            return $"Queue ETL to {ConnectionStringName}";
        }

        internal override EtlConfigurationCompareDifferences Compare(EtlConfiguration<QueueConnectionString> config, Dictionary<string, QueueConnectionString> connectionStrings, List<(string TransformationName, EtlConfigurationCompareDifferences Difference)> transformationDiffs = null)
        {
            var diff = base.Compare(config, connectionStrings, transformationDiffs);

            if (config is QueueEtlConfiguration queueConfig)
            {
                if (queueConfig.BrokerType != BrokerType ||
                    queueConfig.SkipAutomaticQueueDeclaration != SkipAutomaticQueueDeclaration ||
                    EnumerableExtension.ContentEqualsIgnoringOrder(queueConfig.Queues, Queues) == false)
                    diff |= EtlConfigurationCompareDifferences.ConfigurationOptions;
            }

            return diff;
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(BrokerType)] = BrokerType;
            json[nameof(SkipAutomaticQueueDeclaration)] = SkipAutomaticQueueDeclaration;
            json[nameof(Queues)] = Queues != null ? new DynamicJsonArray(Queues.Select(x => x.ToJson())) : null;

            return json;
        }

        public override DynamicJsonValue ToAuditJson()
        {
            return ToJson();
        }
    }

    public class EtlQueue
    {
        public string Name { get; set; }

        public bool DeleteProcessedDocuments { get; set; }

        private bool Equals(EtlQueue other)
        {
            return string.Equals(Name, other.Name) && DeleteProcessedDocuments == other.DeleteProcessedDocuments;
        }

        public override bool Equals(object obj)
        {
            return ReferenceEquals(this, obj) || obj is EtlQueue other && Equals(other);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Name, DeleteProcessedDocuments);
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(DeleteProcessedDocuments)] = DeleteProcessedDocuments
            };
        }
    }
}
