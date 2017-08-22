using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Documents.Patching
{
    public class PatchAndDeleteByCollection : RavenTestBase
    {
        [Theory]
        [InlineData(100)]
        [InlineData(1300)]
        public void CanDeleteCollection(int count)
        {
            using (var store = GetDocumentStore())
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

                var stats = store.Admin.Send(new GetStatisticsOperation());
                Assert.Equal(0, stats.CountOfDocuments);
            }
        }

        [Theory]
        [InlineData(100)]
        [InlineData(1300)]
        public void CanPatchCollection(int count)
        {
            using (var store = GetDocumentStore())
            {
                using (var x = store.OpenSession())
                {
                    for (int i = 0; i < count; i++)
                    {
                        x.Store(new User { }, "users|");
                    }
                    x.SaveChanges();
                }

                var operation = store.Operations.Send(new PatchByQueryOperation(new IndexQuery() {Query = "FROM Users"},  new PatchRequest
                {
                    Script = "this.Name = id(this)"
                }));
                operation.WaitForCompletion(TimeSpan.FromSeconds(30));

                var stats = store.Admin.Send(new GetStatisticsOperation());
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
