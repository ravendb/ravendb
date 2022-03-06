using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17698 : ClusterTestBase
    {
        public RavenDB_17698(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_fail_over_etl_task()
        {
            var (nodes, leader) = await CreateRaftCluster(3, customSettings:
                new Dictionary<string, string>
                {
                    {"ETL.MaxNumberOfExtractedDocuments", "5"}
                });
            var database = GetDatabaseName();

            await CreateDatabaseInClusterInner(new DatabaseRecord(database), 3, leader.WebUrl, null);

            using (var destination = GetDocumentStore())
            using (var source = new DocumentStore
            {
                Database = database,
                Urls = new[] { leader.WebUrl }
            }.Initialize())
            {
                string lastDocumentId = null;
                using (var bulkInsert = source.BulkInsert())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        lastDocumentId = i.ToString();
                        await bulkInsert.StoreAsync(new User(), lastDocumentId);
                    }
                }

                var putResult = await source.Maintenance.SendAsync(new PutConnectionStringOperation<RavenConnectionString>(new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = new[] { destination.Urls.First() },
                    Database = destination.Database,
                }));
                Assert.NotNull(putResult.RaftCommandIndex);

                var configuration = new RavenEtlConfiguration
                {
                    ConnectionStringName = "test",
                    Name = "myConfiguration",
                    Transforms = {
                        new Transformation
                        {
                            Name = "allDocs",
                            Collections = { "Users" }
                        }
                    },
                    MentorNode = "A"
                };

                var addResult = await source.Maintenance.SendAsync(new AddEtlOperation<RavenConnectionString>(configuration));
                Assert.NotNull(addResult.RaftCommandIndex);
                Assert.True(WaitForDocument(destination, lastDocumentId));

                configuration.MentorNode = "B";
                var updateResult = await source.Maintenance.SendAsync(new UpdateEtlOperation<RavenConnectionString>(addResult.TaskId, configuration));
                Assert.NotNull(updateResult.RaftCommandIndex);

                using (var session = source.OpenAsyncSession())
                {
                    var user = new User();
                    await session.StoreAsync(user);
                    lastDocumentId = user.Id;
                    await session.SaveChangesAsync();
                }

                Assert.True(WaitForDocument(destination, lastDocumentId));
            }
        }
    }
}
