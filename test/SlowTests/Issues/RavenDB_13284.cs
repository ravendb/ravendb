using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13284 : ReplicationTestBase
    {
        public RavenDB_13284(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ExternalReplicationCanReestablishAfterServerRestarts(Options options)
        {
            var serverSrc = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false
            });
            var serverDst = GetNewServer(new ServerCreationOptions
            {
                RunInMemory = false
            });
            using (var storeSrc = GetDocumentStore(new Options(options)
            {
                Server = serverSrc,
                Path = Path.Combine(serverSrc.Configuration.Core.DataDirectory.FullPath, "ExternalReplicationCanReestablishAfterServerRestarts")
            }))
            using (var storeDst = GetDocumentStore(new Options(options)
            {
                Server = serverDst,
                Path = Path.Combine(serverDst.Configuration.Core.DataDirectory.FullPath, "ExternalReplicationCanReestablishAfterServerRestarts")
            }))
            {
                await SetupReplicationAsync(storeSrc, storeDst);
                using (var session = storeSrc.OpenSession())
                {
                    session.Store(new User(), "user/1");
                    session.SaveChanges();
                }
                Assert.True(WaitForDocument(storeDst, "user/1"));

                // Taking down destination server
                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(serverDst);
                var settings = new Dictionary<string, string>
                {
                    {RavenConfiguration.GetKey(x => x.Core.ServerUrls), result.Url}
                };
                // Put document while destination is down
                using (var session = storeSrc.OpenSession())
                {
                    session.Store(new User(), "user/2");
                    session.SaveChanges();
                }

                // Bring destination server up
                serverDst = GetNewServer(new ServerCreationOptions
                {
                    RunInMemory = false,
                    DeletePrevious = false,
                    DataDirectory = result.DataDirectory,
                    CustomSettings = settings
                });

                Assert.True(WaitForDocument(storeDst, "user/2"));
            }
        }
    }
}
