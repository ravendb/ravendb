using System;
using System.Linq;
using FastTests;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Operations.Databases;
using Raven.NewClient.Operations.Databases.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace NewClientTests.NewClient.FastTests.Patching
{
    public class PatchAndDeleteByCollection : RavenNewTestBase
    {
        [Fact]
        public void CanDeleteCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var x = store.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        x.Store(new User { }, "users/");
                    }
                    x.SaveChanges();
                }

                var operation = store.Operations.Send(new DeleteByCollectionOperation("users"));
                operation.WaitForCompletion(TimeSpan.FromSeconds(60));

                var stats = store.Admin.Send(new GetStatisticsOperation());

                Assert.Equal(0, stats.CountOfDocuments);
            }
        }

        [Fact]
        public void CanPatchCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var x = store.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        x.Store(new User { }, "users/");
                    }
                    x.SaveChanges();
                }

                var operation = store.Operations.Send(new PatchByCollectionOperation("users", new PatchRequest { Script = " this.Name = __document_id;" }));
                operation.WaitForCompletion(TimeSpan.FromSeconds(60));

                var stats = store.Admin.Send(new GetStatisticsOperation());

                Assert.Equal(100, stats.CountOfDocuments);
                using (var x = store.OpenSession())
                {
                    var users = x.Load<User>(Enumerable.Range(1, 100).Select(i => "users/" + i));
                    Assert.Equal(100, users.Count);

                    foreach (var user in users.Values)
                    {
                        Assert.NotNull(user.Name);
                    }
                }
            }
        }
    }
}