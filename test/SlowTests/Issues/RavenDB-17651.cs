using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;
using Company = SlowTests.Core.Utils.Entities.Company;

namespace SlowTests.Issues
{
    public class RavenDB_17651 : RavenTestBase
    {
        public RavenDB_17651(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_Use_Any_For_A_CollectionQuery_With_An_Id()
        {
            using (var store = GetDocumentStore())
            {
                string companyId;
                using (var session = store.OpenAsyncSession())
                {
                    var company = new Company();
                    await session.StoreAsync(company);
                    await session.StoreAsync(new Order());
                    companyId = company.Id;
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var hasOrder = await session.Query<Order>().AnyAsync(x => x.Id == companyId);
                    Assert.False(hasOrder);
                }
            }
        }
    }
}
