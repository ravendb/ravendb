using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Patching
{
    public class PatchAndDeleteByCollection : RavenTestBase
    {
        public PatchAndDeleteByCollection(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [RavenData(100, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(1300, DatabaseMode = RavenDatabaseMode.All)]
        public void CanDeleteCollection(Options options, int count)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var x = store.OpenSession())
                {
                    for (int i = 0; i < count; i++)
                    {
                        x.Store(new User { }, "users/");
                    }
                    x.SaveChanges();
                }

                var operation = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery { Query = "FROM users" }));
                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                var tester = store.Maintenance.ForTesting(() => new GetStatisticsOperation());

                tester.AssertAll((_, stats) => Assert.Equal(0, stats.CountOfDocuments));
            }
        }

        [Theory]
        [InlineData(100, "Jint")]
        [InlineData(1300, "Jint")]
        [InlineData(100, "V8")]
        [InlineData(1300, "V8")]
        public void CanPatchCollection(int count, string jsEngineType)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var x = store.OpenSession())
                {
                    for (int i = 0; i < count; i++)
                    {
                        x.Store(new User { }, "users|");
                    }
                    x.SaveChanges();
                }
                
                var operation = store.Operations.Send(new PatchByQueryOperation(new IndexQuery() {Query = "FROM Users UPDATE { this.Name = id(this) } " }));
                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                var stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.True(stats.LastDocEtag >= 2 * count);
                Assert.Equal(count, stats.CountOfDocuments);

                using (var session = store.OpenSession())
                {
                    for (int i = 1; i < count; i += 100)
                    {
                        var users = session.Load<User>(Enumerable.Range(i, 100).Select(x => "users/" + x));

                        Assert.Equal(100, users.Count);

                        foreach (var user in users)
                        {
                            Assert.NotNull(user.Value.Name);
                        }
                    }
                }
            }
        }
    }
}
