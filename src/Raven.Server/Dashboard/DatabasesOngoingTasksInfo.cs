using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard
{
    public class DatabasesOngoingTasksInfo : AbstractDashboardNotification
    {
        public override DashboardNotificationType Type => DashboardNotificationType.OngoingTasks;

        public List<DatabaseOngoingTasksInfoItem> Items { get; set; }

        public DatabasesOngoingTasksInfo()
        {
            Items = new List<DatabaseOngoingTasksInfoItem>();
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Items)] = new DynamicJsonArray(Items.Select(x => x.ToJson()));
            return json;
        }

        public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
        {
            var items = new DynamicJsonArray();
            foreach (var databaseInfoItem in Items)
            {
                if (filter(databaseInfoItem.Database, requiresWrite: false))
                {
                    items.Add(databaseInfoItem.ToJson());
                }
            }

            if (items.Count == 0)
                return null;

            var json = base.ToJson();
            json[nameof(Items)] = items;
            return json;
        }
    }

    public class DatabaseOngoingTasksInfoItem : IDynamicJson
    {
        public string Database { get; set; }

        public long ExternalReplicationCount { get; set; }

        public long ReplicationHubCount { get; set; }

        public long ReplicationSinkCount { get; set; }

        public long RavenEtlCount { get; set; }

        public long SqlEtlCount { get; set; }
        
        public long ElasticSearchEtlCount { get; set; }
        
        public long OlapEtlCount { get; set; }

        public long PeriodicBackupCount { get; set; }

        public long SubscriptionCount { get; set; }

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
                [nameof(PeriodicBackupCount)] = PeriodicBackupCount,
                [nameof(SubscriptionCount)] = SubscriptionCount
            };
        }
    }
}
