using System;
using System.Linq;
using System.Threading;
using Raven.Client.Documents.Operations.ETL;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_10674 : EtlTestBase
    {
        public RavenDB_10674(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void EntersFallbackModeIfCantConnectTheDestination()
        {
            using (var src = GetDocumentStore())
            {
                using (var store = src.OpenSession())
                {
                    store.Store(new User());

                    store.SaveChanges();
                }

                var configuration = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "aaa",
                    Transforms =
                    {
                        new Transformation()
                        {
                            Collections = {"Users"},
                            Name = "test"
                        }
                    }
                };

                AddEtl(src, configuration, new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = new []{ "http://abc.localhost:1234"},
                    Database = "test",
                });

                var process = GetDatabase(src.Database).Result.EtlLoader.Processes.First();

                Assert.True(SpinWait.SpinUntil(() =>
                {
                    if (process.FallbackTime != null)
                        return true;

                    Thread.Sleep(100);

                    return false;
                }, TimeSpan.FromMinutes(1)));
            }
        }
    }
}
