using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class JoelAsync : RavenTestBase
    {
        public JoelAsync(ITestOutputHelper output) : base(output)
        {
        }

        private class Dummy
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public async Task AsyncQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var results = await session.Query<Dummy>().ToListAsync();

                    var results2 = await session.Query<Dummy>().ToListAsync();
                
                    Assert.Equal(0, results2.Count);
                }
            }
        }
    }
}
