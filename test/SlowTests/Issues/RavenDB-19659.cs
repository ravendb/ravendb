using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Identity;
using Raven.Server;
using Raven.Server.Config;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19659 : ClusterTestBase
    {
        public RavenDB_19659(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Unique_document_ids_using_hilo()
        {
            var databaseName = GetDatabaseName();
            ;
            (List<RavenServer> Nodes, RavenServer Leader) raftCluster = await CreateRaftCluster(2, shouldRunInMemory: false);
            await CreateDatabaseInCluster(databaseName, 2, raftCluster.Leader.WebUrl);

            using (var store1 = new DocumentStore
            {
                Database = databaseName,
                Urls = new[]
                {
                    raftCluster.Nodes[0].WebUrl,
                    raftCluster.Nodes[1].WebUrl
                }
            })
            using (var store2 = new DocumentStore
            {
                Database = databaseName,
                Urls = new[]
                       {
                           raftCluster.Nodes[0].WebUrl,
                           raftCluster.Nodes[1].WebUrl
                       }
            })
            {
                store1.Initialize();
                store2.Initialize();

                var result0 = await DisposeServerAndWaitForFinishOfDisposalAsync(raftCluster.Nodes[0]);
                var settings0 = new Dictionary<string, string>
                {
                    {RavenConfiguration.GetKey(x => x.Core.ServerUrls), result0.Url}
                };

                var hiLoKeyGenerator1 = new DefaultAsyncHiLoIdGenerator("users", store1, store1.Database, store1.Conventions.IdentityPartsSeparator);
                var hiLoKeyGenerator2 = new DefaultAsyncHiLoIdGenerator("users", store2, store2.Database, store2.Conventions.IdentityPartsSeparator);

                var uniqueIds = new HashSet<string>();
                for (var i = 0; i < 32; i++)
                {

                    var id = await hiLoKeyGenerator1.GenerateDocumentIdAsync(new User());
                    uniqueIds.Add(id);
                }

                var mre = new ManualResetEvent(false);

                var result1 = await DisposeServerAndWaitForFinishOfDisposalAsync(raftCluster.Nodes[1]);
                var settings1 = new Dictionary<string, string>
                {
                    {RavenConfiguration.GetKey(x => x.Core.ServerUrls), result1.Url}
                };

                using (GetNewServer(new ServerCreationOptions
                {
                    RunInMemory = false,
                    DeletePrevious = false,
                    DataDirectory = result0.DataDirectory,
                    CustomSettings = settings0
                }))
                {
                    for (var i = 0; i < 31; i++)
                    {
                        var id = await hiLoKeyGenerator2.GenerateDocumentIdAsync(new User());
                        uniqueIds.Add(id);
                    }
                }

                hiLoKeyGenerator2.ForTestingPurposesOnly().BeforeGeneratingDocumentId = () =>
                {
                    // do it only once
                    hiLoKeyGenerator2.ForTestingPurposesOnly().BeforeGeneratingDocumentId = null;
                    mre.WaitOne();
                };

                var generateIdTask = Task.Run(async () => await hiLoKeyGenerator2.GenerateDocumentIdAsync(new User()));

                using (GetNewServer(new ServerCreationOptions
                {
                    RunInMemory = false,
                    DeletePrevious = false,
                    DataDirectory = result1.DataDirectory,
                    CustomSettings = settings1
                }))
                {
                    var id = await hiLoKeyGenerator2.GenerateDocumentIdAsync(new User());
                    uniqueIds.Add(id);
                    mre.Set();
                }

                var newId = await generateIdTask;
                uniqueIds.Add(newId);

                Assert.Contains("users/32-A", uniqueIds);
                Assert.Equal(65, uniqueIds.Count);
            }
        }
    }
}
