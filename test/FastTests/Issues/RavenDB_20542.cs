using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_20542 : RavenTestBase
    {
        public RavenDB_20542(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void AddLongSkipToLINQ()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "AA" });
                    session.Store(new User { Name = "BB" });
                    session.Store(new User { Name = "CC" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    long x = 1;
                    List<User> users = session.Query<User>().Skip(x).ToList();
                    Assert.Equal(2, users.Count);
                    users = session.Query<User>().Skip(long.MaxValue).ToList();
                    Assert.Equal(0, users.Count);
                }
            }
        }
    }
}
