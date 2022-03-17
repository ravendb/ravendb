using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17311 : ReplicationTestBase
    {
        public RavenDB_17311(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanRemoveAndReAddNodeToDbWhileUpdatingEtlProcessState()
        {
            const string mentor = "A";
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);

            using (var src = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 3
            }))
            using (var dest = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 3
            }))
            {
                var urls = nodes.Select(n => n.WebUrl).ToArray();
                await AddEtl(src, dest.Database, urls, mentor);

                using (var session = src.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    }, "users/1");
                    session.SaveChanges();
                }

                var deletion = await src.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(src.Database, hardDelete: true, fromNode: mentor,
                    timeToWaitForConfirmation: TimeSpan.FromSeconds(30)));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(deletion.RaftCommandIndex, TimeSpan.FromSeconds(30));

                using (var session = src.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1);

                    session.Store(new User()
                    {
                        Name = "John Doe"
                    }, "users/2");
                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, "users/2", u => u.Name == "John Doe", 30_000));

                var addResult = await src.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(src.Database, node: mentor));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(addResult.RaftCommandIndex, TimeSpan.FromSeconds(30));

                await WaitAndAssertForValueAsync(() => GetMembersCount(src), 3);
            }
        }

        internal static async Task<AddEtlOperationResult> AddEtl(IDocumentStore source, string destination, string[] urls, string mentor)
        {
            var connectionStringName = $"RavenEtl_From{source.Database}_To{destination}";
            var config = new RavenEtlConfiguration()
            {
                Name = connectionStringName,
                ConnectionStringName = connectionStringName,
                LoadRequestTimeoutInSec = 10,
                MentorNode = mentor,
                Transforms = new List<Transformation>
                {
                    new Transformation
                    {
                        Name = $"ETL : {connectionStringName}",
                        ApplyToAllDocuments = true,
                        IsEmptyScript = true
                    }
                }
            };
            var connectionString = new RavenConnectionString
            {
                Name = connectionStringName,
                Database = destination,
                TopologyDiscoveryUrls = urls,
            };

            var result = await source.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
            Assert.NotNull(result.RaftCommandIndex);

            return await source.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(config));
        }
    }
}
