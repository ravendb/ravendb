using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16782 : RavenTestBase
    {
        public RavenDB_16782(ITestOutputHelper output) : base(output)
        {
        }

        public async Task ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var dateTime = "2015-10-17T13:28:17-05:00";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        DateTime = dateTime
                    });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var result = await session.Query<User>().Where(x => x.DateTime == dateTime).ToListAsync();
                    Assert.Equal(1, result.Count);

                    result = await session.Query<User>().Where(x => x.DateTime == "2015-10-17T13:28:17-04:00").ToListAsync();
                    Assert.Equal(0, result.Count);
                }
            }
        }

        private class User
        {
            public string DateTime { get; set; }
        }
    }
}
