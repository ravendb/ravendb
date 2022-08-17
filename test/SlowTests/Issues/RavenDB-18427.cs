using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Json;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18427 : RavenTestBase
    {
        public RavenDB_18427(ITestOutputHelper output) : base(output)
        {
        }
        [Fact]
        public void Store_Documents2()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User { Name = "Foo/Bar" };
                    session.Store(user, "foo");
                    session.Store(user, "Foo");
                    Assert.Throws<InvalidOperationException>(() => session.Store(user, "bar"));
                    session.SaveChanges();

                    var usersCount = session.Query<User>().Count();
                    Assert.Equal(usersCount, 1);

                    var user1 = session.Load<User>("foo");
                    Assert.NotNull(user1);
                    var user2 = session.Load<User>("bar");
                    Assert.Null(user2);
                }
            }
        }
    }
}
