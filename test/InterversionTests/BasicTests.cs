using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests
{
    public class BasicTests : InterversionTestBase
    {
        public BasicTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Test()
        {
            var getStoreTask407 = GetDocumentStoreAsync("4.0.7");
            var getStoreTask408 = GetDocumentStoreAsync("4.0.8");

            await Task.WhenAll(getStoreTask407, getStoreTask408);

            AssertStore(await getStoreTask407);
            AssertStore(await getStoreTask408);
            AssertStore(GetDocumentStore());
        }

        private static void AssertStore(IDocumentStore store)
        {
            using (store)
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "HR"
                    }, "companies/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var c = session.Load<Company>("companies/1");
                    Assert.NotNull(c);
                    Assert.Equal("HR", c.Name);
                }
            }
        }
    }
}
