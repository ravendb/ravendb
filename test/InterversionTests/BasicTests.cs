using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace InterversionTests
{
    public class BasicTests : InterversionTestBase
    {
        [Fact]
        public async Task Test()
        {
            var getStoreTask1 = GetDocumentStoreAsync("4.0.7-nightly-20180818-0400");
            var getStoreTask2 = GetDocumentStoreAsync("4.0.7-nightly-20180820-0400");

            await Task.WhenAll(getStoreTask1, getStoreTask2);

            AssertStore(await getStoreTask1);
            AssertStore(await getStoreTask2);
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
