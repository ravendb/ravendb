using System;
using System.Threading.Tasks;
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
        public async Task DeletingDocumentWithRevisionsDoesntCorruptEtlProcess()
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

                var loadDone1 = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.LoadSuccesses == 1);
                var loadDone2 = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.LoadSuccesses == 2);
                var deleteDone = Etl.WaitForEtlToComplete(src, (_, statistics) => statistics.LoadSuccesses == 3);

                using (var session = src.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Bob" }, "users/1");
                    await session.SaveChangesAsync();
                }

                if (loadDone1.Wait(TimeSpan.FromSeconds(30)) == false)
                {
                    var loadError = await Etl.TryGetLoadErrorAsync(src.Database, configuration);
                    var transformationError = await Etl.TryGetTransformationErrorAsync(src.Database, configuration);

                    Assert.Fail($"ETL wasn't done. Load error: {loadError?.Error}. Transformation error: {transformationError?.Error}");
                }

                using (var session = dst.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("Bob", user.Name);
                }

                using (var session = src.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    user.Name = "Gracjan";
                    await session.SaveChangesAsync();
                }

                if (loadDone2.Wait(TimeSpan.FromSeconds(30)) == false)
                {
                    var loadError = await Etl.TryGetLoadErrorAsync(src.Database, configuration);
                    var transformationError = await Etl.TryGetTransformationErrorAsync(src.Database, configuration);

                    Assert.Fail($"ETL wasn't done. Load error: {loadError?.Error}. Transformation error: {transformationError?.Error}");
                }

                using (var session = dst.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal("Gracjan", user.Name);
                }

                using (var session = src.OpenAsyncSession())
                {
                    session.Delete("users/1");
                    await session.SaveChangesAsync();
                }

                Assert.True(deleteDone.Wait(TimeSpan.FromSeconds(30)));

                using (var session = dst.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Null(user);
                }
            }
        }
    }
}
