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
            var getStoreTask405 = GetDocumentStoreAsync("4.0.5");
            var getStoreTask406Patch = GetDocumentStoreAsync("4.0.6-patch-40047");

            await Task.WhenAll(getStoreTask405, getStoreTask406Patch);

            AssertStore(await getStoreTask405);
            AssertStore(await getStoreTask406Patch);
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
