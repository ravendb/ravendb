using System;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8397 : RavenTestBase
    {
        [Fact]
        public void PathAndDeleteByQueryWithFilteringById()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Joe" }, "users/1");
                    session.Store(new User { Name = "Doe" }, "users/2");

                    session.SaveChanges();
                }

                var patchOp = store.Operations.Send(new PatchByQueryOperation("FROM Users u WHERE id(u) = 'users/1' UPDATE { this.LastName = 'Smith'; }"));

                patchOp.WaitForCompletion(TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.Equal("Smith", user.LastName);

                    user = session.Load<User>("users/2");
                    Assert.Null(user.LastName);
                }

                patchOp = store.Operations.Send(new PatchByQueryOperation("FROM @all_docs WHERE id() IN ('users/2') UPDATE { this.LastName = 'Davis'; }"));

                patchOp.WaitForCompletion(TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.Equal("Smith", user.LastName);

                    user = session.Load<User>("users/2");
                    Assert.Equal("Davis", user.LastName);
                }

                var deleteOp = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery { Query = "FROM Users u WHERE id(u) IN ('users/1')" }));

                deleteOp.WaitForCompletion(TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");
                    Assert.Null(user);

                    user = session.Load<User>("users/2");
                    Assert.NotNull(user);
                }

                deleteOp = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery { Query = "FROM @all_docs WHERE id() = 'users/2'" }));

                deleteOp.WaitForCompletion(TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/2");
                    Assert.Null(user);
                }
            }
        }
    }
}
