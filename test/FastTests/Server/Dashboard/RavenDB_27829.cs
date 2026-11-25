using System.Collections.Generic;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Dashboard.Cluster.Notifications;
using Raven.Server.Storage;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Dashboard
{
    public class RavenDB_27829 : NoDisposalNeeded
    {
        public RavenDB_27829(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Monitoring)]
        public void Disk_status_on_the_io_widget_is_sent_to_operators_only()
        {
            var payload = new IoStatsPayload
            {
                Items = new List<IoStatsResult>(),
                StorageStatus = new StorageStatusInfo { Status = StorageDeviceStatus.Saturated, Summary = "Disk / is saturated" }
            };

            // a null filter is what Operators, ClusterAdmins and unsecured servers get
            Assert.NotNull(payload.ToJsonWithFilter(filter: null)[nameof(IoStatsPayload.StorageStatus)]);

            // a user with access to some databases
            var json = payload.ToJsonWithFilter((database, _) => database == "Orders");
            Assert.Null(json[nameof(IoStatsPayload.StorageStatus)]);
            Assert.NotNull(json[nameof(IoStatsPayload.Items)]);
        }
    }
}
