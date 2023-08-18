using System;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Operations.ETL;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_20136 : RavenTestBase
    {

        public RavenDB_20136(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async void DeletingDocumentWithRevisionsDoesntCorruptETLProcess()
        {
            using (var src = GetDocumentStore())
            using (var dst = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, src.Database);
                var configuration = new RavenEtlConfiguration
                {
                    ConnectionStringName = "test", Name = "aaa", Transforms = {new Transformation {Name = "S1", Collections = {"Users"}}}
                };

                Etl.AddEtl(src, configuration, new RavenConnectionString {Name = "test", TopologyDiscoveryUrls = dst.Urls, Database = dst.Database,});

                var etlDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.LoadSuccesses == 3);
                var loadDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.LoadSuccesses == 2);

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }
                
                using (var session = src.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    user.Name = "Gracjan";
                    session.SaveChanges();
                }
                
                Assert.True(loadDone.Wait(TimeSpan.FromSeconds(30)));
                
                using (var session = dst.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.NotNull(user);
                }
                
                using (var session = src.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }
                
                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(30)));
                
                using (var session = dst.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.Null(user);
                }
            }
        }
    }
}
