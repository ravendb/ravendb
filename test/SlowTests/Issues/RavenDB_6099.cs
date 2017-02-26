using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_6099 : RavenTestBase
    {
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

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var operation = session.Advanced.PatchByIndex<User, Users_ByAge>(x => x.Age > 11, new PatchRequest
                    {
                        Script = "this.Name = 'Patched';"
                    });

                    operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                    var users = session.Load<User>(new[] { "users/1", "users/2", "users/3" });

                    Assert.Equal(3, users.Count);
                    Assert.Null(users["users/1"].Name);
                    Assert.Equal("Patched", users["users/2"].Name);
                    Assert.Equal("Patched", users["users/3"].Name);
                }
            }
        }
    }
}