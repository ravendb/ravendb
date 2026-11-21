using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Commands.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_28079 : ClusterTestBase
    {
        public RavenDB_28079(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Indexes)]
        public void RollingIndexDeploymentDoesNotAddNodesThatJoinedAfterItStarted()
        {
            var createdAt = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            var record = CreateRecordWithRollingIndex(createdAt);
            new PutRollingIndexCommand(record.DatabaseName, "Users/ByName", "A", createdAt.AddMinutes(5), "finish").UpdateDatabaseRecord(record, 10);
            Assert.Equal(["A", "B"], record.RollingIndexes["Users/ByName"].ActiveDeployments.Keys.Order());

            record = CreateRecordWithRollingIndex(createdAt);
            new PutRollingIndexCommand(record.DatabaseName, "Users/ByName", "A", finishedAt: null, "dummy update").UpdateDatabaseRecord(record, 10);
            Assert.Equal(["A", "B"], record.RollingIndexes["Users/ByName"].ActiveDeployments.Keys.Order());
        }

        private static DatabaseRecord CreateRecordWithRollingIndex(DateTime createdAt)
        {
            var record = new DatabaseRecord("db")
            {
                Topology = new DatabaseTopology
                {
                    Members = new List<string> { "A", "B", "C" }
                },
                RollingIndexes = new Dictionary<string, RollingIndex>()
            };

            var rollingIndex = new RollingIndex { RaftCommandIndex = 5 };
            rollingIndex.ActiveDeployments["A"] = new RollingIndexDeployment
            {
                State = RollingIndexState.Running,
                CreatedAt = createdAt,
                StartedAt = createdAt
            };
            rollingIndex.ActiveDeployments["B"] = new RollingIndexDeployment
            {
                State = RollingIndexState.Pending,
                CreatedAt = createdAt
            };
            record.RollingIndexes["Users/ByName"] = rollingIndex;

            return record;
        }
    }
}
