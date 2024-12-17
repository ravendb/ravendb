using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.ETL;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_10674 : RavenTestBase
    {
        public RavenDB_10674(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task EntersFallbackModeIfCantConnectTheDestination()
        {
            using (var src = GetDocumentStore())
            {
                using (var store = src.OpenAsyncSession())
                {
                    await store.StoreAsync(new User());
                    await store.SaveChangesAsync();
                }

                var configuration = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "aaa",
                    Transforms =
                    {
                        new Transformation
                        {
                            Collections = {"Users"},
                            Name = "test"
                        }
                    }
                };

                Etl.AddEtl(src, configuration, new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = new []{ "http://abc.localhost:1234"},
                    Database = "test",
                });

                var process = (await GetDatabase(src.Database)).EtlLoader.Processes[0];

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
