using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding
{
    public class FailoverEtlTests : ClusterTestBase
    {
        public FailoverEtlTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ReplicateFromSingleSource()
        {
            var srcDb = "ReplicateFromSingleSourceSrc";
            var dstDb = "ReplicateFromSingleSourceDst";
            var (_, srcRaft) = await CreateRaftCluster(3);
            var (_, dstRaft) = await CreateRaftCluster(1);

            var record = new DatabaseRecord(srcDb)
            {
                Shards = new[]
                {
                    new DatabaseTopology(), 
                    new DatabaseTopology(), 
                    new DatabaseTopology()
                }
            };
            var srcNodes = await CreateDatabaseInClusterInner(record, 3, srcRaft.WebUrl, certificate: null);
            var destNode = await CreateDatabaseInCluster(dstDb, 1, dstRaft.WebUrl);
            var node = srcNodes.Servers.First(x => x.ServerStore.NodeTag != srcRaft.ServerStore.NodeTag).ServerStore.NodeTag;

            using (var src = new DocumentStore()
            {
                Urls = srcNodes.Servers.Select(s => s.WebUrl).ToArray(),
                Database = srcDb,
            }.Initialize())
            using (var dest = new DocumentStore
            {
                Urls = new[] { destNode.Servers[0].WebUrl },
                Database = dstDb,
            }.Initialize())
            {
                var connectionStringName = "EtlFailover";
                var urls = new[] { destNode.Servers[0].WebUrl };
                var config = new RavenEtlConfiguration()
                {
                    Name = connectionStringName,
                    ConnectionStringName = connectionStringName,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = $"ETL : {connectionStringName}",
                            Collections = new List<string>(new[] {"Users"}),
                            Script = null,
                            ApplyToAllDocuments = false,
                            Disabled = false
                        }
                    },
                    LoadRequestTimeoutInSec = 30,
                    MentorNode = node
                };
                var connectionString = new RavenConnectionString
                {
                    Name = connectionStringName,
                    Database = dest.Database,
                    TopologyDiscoveryUrls = urls,
                };

                var result = src.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
                Assert.NotNull(result.RaftCommandIndex);

                src.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(config));
                var originalTaskNode = srcNodes.Servers.Single(s => s.ServerStore.NodeTag == node);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe"
                    }, "users/1");

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, "users/1", u => u.Name == "Joe Doe", 30_000));
                await ActionWithLeader((l) => l.ServerStore.RemoveFromClusterAsync(node), srcNodes.Servers);
                Thread.Sleep(100000);
                await originalTaskNode.ServerStore.WaitForState(RachisState.Passive, CancellationToken.None);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe Doe2"
                    }, "users/2");

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, "users/2", u => u.Name == "Joe Doe2", 60_000));
                Assert.Throws<NodeIsPassiveException>(() =>
                {
                    using (var originalSrc = new DocumentStore
                    {
                        Urls = new[] { originalTaskNode.WebUrl },
                        Database = srcDb,
                        Conventions = new DocumentConventions
                        {
                            DisableTopologyUpdates = true
                        }
                    }.Initialize())
                    {
                        using (var session = originalSrc.OpenSession())
                        {
                            session.Store(new User()
                            {
                                Name = "Joe Doe3"
                            }, "users/3");

                            session.SaveChanges();
                        }
                    }
                });
            }
        }
    }
}
