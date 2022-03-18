using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6099 : RavenTestBase
    {
        public RavenDB_6099(ITestOutputHelper output) : base(output)
        {
        }

        private class Users_ByAge : AbstractIndexCreationTask<User>
        {
            public Users_ByAge()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Age
                               };
            }
        }

        [Fact]
        public void Session_PatchByIndex()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Age = 10 });
                    session.Store(new User { Age = 14 });
                    session.Store(new User { Age = 17 });

                    session.SaveChanges();
                }

                new Users_ByAge().Execute(store);

                Indexes.WaitForIndexing(store);

                var operation = store.Operations.Send(new PatchByQueryOperation(
                    "from index 'Users/ByAge' as u where u.Age > 11 update { u.Name = 'Patched'; } "
                ));

                operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession())
                {
                    var users = session.Load<User>(new[] { "users/1-A", "users/2-A", "users/3-A" });

                    Assert.Equal(3, users.Count);
                    Assert.Null(users["users/1-A"].Name);
                    Assert.Equal("Patched", users["users/2-A"].Name);
                    Assert.Equal("Patched", users["users/3-A"].Name);
                }
            }
        }
    }
}
