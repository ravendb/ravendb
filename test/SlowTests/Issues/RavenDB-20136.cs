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
        public async void DeletingDocumentWithRevisionsDoesntCorruptEtlProcess()
        {
            using (var src = GetDocumentStore())
            using (var dst = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisionsAsync(Server.ServerStore, src.Database);
                var configuration = new RavenEtlConfiguration
                {
                    ConnectionStringName = "test",
                    Name = "aaa",
                    Transforms = { new Transformation { Name = "S1", Collections = { "Users" } } }
                };

                Etl.AddEtl(src, configuration, new RavenConnectionString { Name = "test", TopologyDiscoveryUrls = dst.Urls, Database = dst.Database, });

                var loadDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.LoadSuccesses == 2);
                var deleteDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.LoadSuccesses == 3);

                using (var session = src.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }
                
                var etlDone = Etl.WaitForEtlToComplete(src);
                if (etlDone.Wait(TimeSpan.FromSeconds(30)) == false)
                {
                    Etl.TryGetLoadError(src.Database, configuration, out var loadError);
                    Etl.TryGetTransformationError(src.Database, configuration, out var transformationError);

                    Assert.Fail($"ETL wasn't done. Load error: {loadError?.Error}. Transformation error: {transformationError?.Error}");
                }

                using (var session = src.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    user.Name = "Gracjan";
                    session.SaveChanges();
                }

                if (loadDone.Wait(TimeSpan.FromSeconds(30)) == false)
                {
                    Etl.TryGetLoadError(src.Database, configuration, out var loadError);
                    Etl.TryGetTransformationError(src.Database, configuration, out var transformationError);

                    Assert.Fail($"ETL wasn't done. Load error: {loadError?.Error}. Transformation error: {transformationError?.Error}");
                }

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

                Assert.True(deleteDone.Wait(TimeSpan.FromSeconds(30)));

                using (var session = dst.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.Null(user);
                }
            }
        }
    }
}
