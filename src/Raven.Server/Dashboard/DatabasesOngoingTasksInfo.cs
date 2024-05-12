using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard
{
    public sealed class DatabasesOngoingTasksInfo : AbstractDashboardNotification
    {
        public List<DatabaseOngoingTasksInfoItem> Items { get; set; }

        public DatabasesOngoingTasksInfo()
        {
            Items = new List<DatabaseOngoingTasksInfoItem>();
        }
    }

    public sealed class DatabaseOngoingTasksInfoItem : IDynamicJson
    {
        public string Database { get; set; }

        public long ExternalReplicationCount { get; set; }

        public long ReplicationHubCount { get; set; }

        public long ReplicationSinkCount { get; set; }

        public long RavenEtlCount { get; set; }

        public long SqlEtlCount { get; set; }
        
        public long ElasticSearchEtlCount { get; set; }
        
        public long OlapEtlCount { get; set; }
        
        public long KafkaEtlCount { get; set; }
        
        public long RabbitMqEtlCount { get; set; }
        
        public long AzureQueueStorageEtlCount { get; set; }

        public long PeriodicBackupCount { get; set; }

        public long SubscriptionCount { get; set; }

        public long KafkaSinkCount { get; set; }

        public long RabbitMqSinkCount { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Database)] = Database,
                [nameof(ExternalReplicationCount)] = ExternalReplicationCount,
                [nameof(ReplicationHubCount)] = ReplicationHubCount,
                [nameof(ReplicationSinkCount)] = ReplicationSinkCount,
                [nameof(RavenEtlCount)] = RavenEtlCount,
                [nameof(SqlEtlCount)] = SqlEtlCount,
                [nameof(ElasticSearchEtlCount)] = ElasticSearchEtlCount,
                [nameof(OlapEtlCount)] = OlapEtlCount,
                [nameof(KafkaEtlCount)] = KafkaEtlCount,
                [nameof(RabbitMqEtlCount)] = RabbitMqEtlCount,
                [nameof(AzureQueueStorageEtlCount)] = AzureQueueStorageEtlCount,
                [nameof(PeriodicBackupCount)] = PeriodicBackupCount,
                [nameof(SubscriptionCount)] = SubscriptionCount,
                [nameof(KafkaSinkCount)] = KafkaSinkCount,
                [nameof(RabbitMqSinkCount)] = RabbitMqSinkCount,
            };
        }
    }
}
