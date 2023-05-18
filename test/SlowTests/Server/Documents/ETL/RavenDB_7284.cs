using System;
using FastTests;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_7284 : RavenTestBase
    {
        public RavenDB_7284(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void EtlTaskDeletionShouldDeleteItsState(Options options)
        {
            using (var src = GetDocumentStore(options))
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                var etlDone = Etl.WaitForEtlToComplete(src);

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

                var result = Etl.AddEtl(src, configuration, new RavenConnectionString()
                {
                    Name = "test",
                    TopologyDiscoveryUrls = dest.Urls,
                    Database = dest.Database,
                });

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                src.Maintenance.Send(new DeleteOngoingTaskOperation(result.TaskId, OngoingTaskType.RavenEtl));

                etlDone.Reset();
                
                Etl.AddEtl(src, configuration, new RavenConnectionString()
                {
                    Name = "test",
                    TopologyDiscoveryUrls = dest.Urls,
                    Database = dest.Database,
                });

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));
            }
        }
    }
}
