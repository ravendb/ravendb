using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8397 : RavenTestBase
    {
        [Fact]
        public void ShouldThrowOnAttemptToFilterByIdUsingBetweenOperator()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var ex = Assert.Throws<InvalidQueryException>(() => session.Advanced.DocumentQuery<Order>().RawQuery("from Orders where id() between 'orders/1' and 'orders/100'").ToList());

                    Assert.Contains("Collection query does not support filtering by id() using Between operator. Supported operators are: =, IN", ex.Message);
                }
            }
        }

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
