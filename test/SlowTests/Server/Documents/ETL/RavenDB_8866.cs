using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;
using Index = SlowTests.Issues.Index;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_8866 : EtlTestBase
    {
        public RavenDB_8866(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanResetEtl()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                var runs = 0;

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                var resetDone = WaitForEtl(src, (n, statistics) => ++runs >= 2);

                var configuration = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "myConfiguration",
                    Transforms =
                    {
                        new Transformation()
                        {
                            Name = "allUsers",
                            Collections = {"Users"}
                        }
                    }
                };

                AddEtl(src, configuration, new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = dest.Urls,
                    Database = dest.Database,
                });

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                src.Maintenance.Send(new ResetEtlOperation("myConfiguration", "allUsers"));

                Assert.True(resetDone.Wait(TimeSpan.FromMinutes(1)));
            }
        }

        [Fact]
        public void CanResetEtl2()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                var configuration = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "myConfiguration",
                    Transforms =
                    {
                        new Transformation()
                        {
                            Name = "allUsers",
                            Collections = {"Users"}
                        }
                    }
                };

                var mre = new ManualResetEvent(true);
                var mre2 = new ManualResetEvent(false);
                var etlDone = WaitForEtl(src, (n, s) =>
                {
                    Assert.True(mre.WaitOne(TimeSpan.FromMinutes(1)));
                    mre.Reset();

                    mre2.Set();

                    return true;
                });

                AddEtl(src, configuration, new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = dest.Urls,
                    Database = dest.Database,
                });

                var set = new HashSet<string>
                {
                    "asd"
                };

                for (int i = 0; i < 10; i++)
                {

                    Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)), $"blah at {i}");

                    mre.Set();

                    Assert.True(mre2.WaitOne(TimeSpan.FromMinutes(1)), $"oops at {i}");
                    mre2.Reset();

                    var t1 = src.Maintenance.SendAsync(new ResetEtlOperation("myConfiguration", "allUsers"));

                    for (int j = 0; j < 100; j++)
                    {
                        var t2 = src.Maintenance.Server.SendAsync(new UpdateUnusedDatabasesOperation(src.Database, set));
                    }
                }
            }
        }

        [Fact(Skip = "RavenDB-14127")]
        public async Task CanResetEtl3()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                var configuration = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "myConfiguration",
                    Transforms = { new Transformation() { Name = "allUsers", Collections = { "Users" } } }
                };

                AddEtl(src, configuration, new RavenConnectionString { Name = "test", TopologyDiscoveryUrls = dest.Urls, Database = dest.Database, });

                var t = Task.Run(async () =>
                {
                    for (int i = 0; i < 100; i++)
                    {
                        await src.Maintenance.SendAsync(new ResetEtlOperation("myConfiguration", "allUsers"));
                        await Task.Delay(100);
                    }
                });

                var indexes = new List<Task>();
                for (int i = 0; i < 100; i++)
                {
                    var index = new Index($"test{i}");
                    indexes.Add(index.ExecuteAsync(src));
                }

                await Task.WhenAll(indexes);
                await t;

                var record = await src.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(src.Database));
                Assert.Equal(100, record.Indexes.Count);
            }
        }
    }
}
