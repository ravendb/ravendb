using System.Collections.Generic;
using System.Linq;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Storage;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public sealed class IoStatsPayload : AbstractClusterDashboardNotification
    {
        public List<IoStatsResult> Items { get; set; }

        // device level state is shared by every database on the disk: Operators only, see ToJsonWithFilter
        public StorageStatusInfo StorageStatus { get; set; }

        public override ClusterDashboardNotificationType Type => ClusterDashboardNotificationType.IoStats;

        public override DynamicJsonValue ToJson() => ToJson(includeStorageStatus: true);

        // a null filter is an Operator / ClusterAdmin (or an unsecured server), see GetDatabaseAccessValidationFunc
        public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter) => ToJson(includeStorageStatus: filter == null);

        private DynamicJsonValue ToJson(bool includeStorageStatus)
        {
            var json = base.ToJson();

            json[nameof(Items)] = new DynamicJsonArray(Items.Select(x => x.ToJson()));
            if (includeStorageStatus)
                json[nameof(StorageStatus)] = StorageStatus?.ToJson();

            return json;
        }
    }
}
