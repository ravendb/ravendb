using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Issues
{
    public class RavenDB_19240 : RavenTestBase
    {
        public RavenDB_19240(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_Use_AsAsyncEnumerable()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new User { Name = "User" + i });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().AsEnumerable();
                    int i = 0;

                    Assert.Equal(0, session.Advanced.NumberOfRequests);

                    foreach (var user in users)
                    {
                        Assert.Equal("User" + i++, user.Name);
                    }

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var users = session.Query<User>().AsAsyncEnumerable();
                    int i = 0;

                    Assert.Equal(0, session.Advanced.NumberOfRequests);

                    await foreach (var user in users)
                    {
                        Assert.Equal("User" + i++, user.Name);
                    }

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
    }
}
