using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class IndexingSpeedPayload : AbstractClusterDashboardNotification
    {
        public double IndexedPerSecond { get; set; }

        public double MappedPerSecond { get; set; }

        public double ReducedPerSecond { get; set; }

        internal List<IndexingSpeedItem> IndexingSpeedPerDatabase { get; set; }

        public override ClusterDashboardNotificationType Type => ClusterDashboardNotificationType.Indexing;

        public override DynamicJsonValue ToJson()
        {
            var result = new IndexingSpeedPayload();

            foreach (IndexingSpeedItem item in IndexingSpeedPerDatabase)
            {
                result.Add(item);
            }

            return result.ToJsonInternal();
        }

        public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
        {
            var result = new IndexingSpeedPayload();

            foreach (IndexingSpeedItem item in IndexingSpeedPerDatabase)
            {
                if (filter(item.Database, requiresWrite: false))
                {
                    result.Add(item);
                }
            }

            return result.ToJsonInternal();
        }

        private DynamicJsonValue ToJsonInternal()
        {
            var json = base.ToJson();

            json[nameof(IndexedPerSecond)] = IndexedPerSecond;
            json[nameof(MappedPerSecond)] = MappedPerSecond;
            json[nameof(ReducedPerSecond)] = ReducedPerSecond;

            return json;
        }

        private void Add(IndexingSpeedItem item)
        {
            IndexedPerSecond += item.IndexedPerSecond;
            MappedPerSecond += item.MappedPerSecond;
            ReducedPerSecond += item.ReducedPerSecond;
        }
    }
}
