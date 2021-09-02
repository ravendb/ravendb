using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_13288 : ReplicationTestBase
    {
        public RavenDB_13288(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldSendCounterChangeMadeInCluster()
        {
            var srcDb = "13288-src";
            var dstDb = "13288-dst";

            var (_, srcRaft) = await CreateRaftCluster(2);
            var (_, dstRaft) = await CreateRaftCluster(1);
            var srcNodes = await CreateDatabaseInCluster(srcDb, 2, srcRaft.WebUrl);
            var destNode = await CreateDatabaseInCluster(dstDb, 1, dstRaft.WebUrl);

            using (var src = new DocumentStore
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
                var connectionStringName = "my-etl";
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
                    MentorNode = "A"
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

                var aNode = srcNodes.Servers.Single(s => s.ServerStore.NodeTag == "A");
                var bNode = srcNodes.Servers.Single(s => s.ServerStore.NodeTag == "B");

                // modify counter on A node (mentor of ETL task)

                using (var aSrc = new DocumentStore
                {
                    Urls = new[] { aNode.WebUrl },
                    Database = srcDb,
                    Conventions = new DocumentConventions
                    {
                        DisableTopologyUpdates = true
                    }
                }.Initialize())
                {
                    using (var session = aSrc.OpenSession())
                    {
                        session.Store(new User()
                        {
                            Name = "Joe Doe"
                        }, "users/1");

                        session.CountersFor("users/1").Increment("likes");

                        session.SaveChanges();
                    }
                }

                Assert.True(WaitForDocument<User>(dest, "users/1", u => u.Name == "Joe Doe", 30_000));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.NotNull(user);
                    Assert.Equal("Joe Doe", user.Name);

                    var counter = session.CountersFor("users/1").Get("likes");

                    Assert.NotNull(counter);
                    Assert.Equal(1, counter.Value);
                }

                // modify counter on B node (not mentor)

                using (var bSrc = new DocumentStore
                {
                    Urls = new[] { bNode.WebUrl },
                    Database = srcDb,
                    Conventions = new DocumentConventions
                    {
                        DisableTopologyUpdates = true
                    }
                }.Initialize())
                {
                    using (var session = bSrc.OpenSession())
                    {
                        session.CountersFor("users/1").Increment("likes");

                        session.SaveChanges();
                    }
                }

                Assert.True(WaitForCounterReplication(new List<IDocumentStore>
                {
                    dest
                }, "users/1", "likes", 2, TimeSpan.FromSeconds(60)));
            }
        }
    }
}
