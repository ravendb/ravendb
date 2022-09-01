using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19016 : RavenTestBase
{
    public RavenDB_19016(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task Can_Index_Nested_Document_Change()
    {
        using (var server = GetNewServer())
        using (var store = GetDocumentStore(new Options { Server = server }))
        {
            const string orderId = "OrdErs/1";
            const string companyId = "CompaNies/1";
            const string userName = "Grisha";
            const string companyName = "Hibernating Rhinos";
            const int orderCount = 10;

            await new UsersIndex().ExecuteAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Name = userName,
                    CompanyId = companyId
                }, "UseRs/1");

                session.Advanced.WaitForIndexesAfterSaveChanges();
                await session.SaveChangesAsync();
            }

            var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            var indexStore = database.IndexStore;

            using (var session = store.OpenAsyncSession())
            {
                var runOnce = false;

                indexStore.ForTestingPurposesOnly().AfterReferencedDocumentWasIndexed = () =>
                {
                    if (runOnce)
                        return;

                    runOnce = true;
                    using (var session2 = store.OpenSession())
                    {
                        session2.Store(new Order
                        {
                            Count = orderCount
                        }, orderId);
                        session2.SaveChanges();
                    }
                };

                await session.StoreAsync(new Company
                {
                    Name = companyName,
                    OrderId = orderId
                }, companyId);

                session.Advanced.WaitForIndexesAfterSaveChanges();
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var results = await session
                    .Query<UsersIndex.Result, UsersIndex>()
                    .ProjectInto<UsersIndex.Result>()
                    .ToListAsync();

                Assert.Equal(1, results.Count);
                Assert.Equal(userName, results[0].UserName);
                Assert.Equal(orderId, results[0].OrderId);
                Assert.Equal(companyId, results[0].CompanyId);
                Assert.Equal(companyName, results[0].CompanyName);
                Assert.Equal(orderCount, results[0].OrderCount);
            }
        }
    }

    private class User
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public string CompanyId { get; set; }
    }

    private class Order
    {
        public string Id { get; set; }

        public int Count { get; set; }
    }

    private class Company
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public string OrderId { get; set; }
    }

    private class UsersIndex : AbstractIndexCreationTask<User>
    {
        public class Result
        {
            public string UserName { get; set; }
            public string OrderId { get; set; }
            public string CompanyId { get; set; }
            public string CompanyName { get; set; }
            public int OrderCount { get; set; }
        }

        public UsersIndex()
        {
            Map = users =>
                from user in users
                let company = LoadDocument<Company>(user.CompanyId)
                let order = LoadDocument<Order>(company.OrderId)
                select new Result
                {
                    UserName = user.Name,
                    OrderId = company.OrderId,
                    CompanyId = user.CompanyId,
                    CompanyName = company.Name,
                    OrderCount = order.Count
                };

            StoreAllFields(FieldStorage.Yes);
        }
    }
}
