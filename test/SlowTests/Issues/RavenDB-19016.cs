using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19016 : RavenTestBase
{
    public RavenDB_19016(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Can_Index_Nested_Document_Change_Different_Collections(Options options)
    {
        using (var server = GetNewServer())
        {
            options.Server = server;
            using (var store = GetDocumentStore(options))
            {
                const string orderId = "OrdErs/1";
                const string companyId = "CompaNies/1";
                const string userName = "Grisha";
                const string companyName = "Hibernating Rhinos";
                const int orderCount = 10;

                var deployedIndex = new UsersIndex();
                await deployedIndex.ExecuteAsync(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = userName, CompanyId = companyId}, "UseRs/1");

                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    await session.SaveChangesAsync();
                }

                var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                var index = database.IndexStore.GetIndex(deployedIndex.IndexName);

                using (var session = store.OpenAsyncSession())
                {
                    index.ForTestingPurposesOnly().BeforeClosingDocumentsReadTransactionForHandleReferences = () =>
                    {
                        index.ForTestingPurposesOnly().BeforeClosingDocumentsReadTransactionForHandleReferences = null;

                        using (var session2 = store.OpenSession())
                        {
                            session2.Store(new Order {Count = orderCount}, orderId);
                            session2.SaveChanges();
                        }
                    };

                    await session.StoreAsync(new Company {Name = companyName, OrderId = orderId}, companyId);

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
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Can_Index_Nested_Document_Change_Same_Collection(Options options)
    {
        using (var server = GetNewServer())
        {
            options.Server = server;
            using (var store = GetDocumentStore(options))
            {
                const string userId1 = "users/1";
                const string userId2 = "users/2";
                const string userId3 = "users/3";
                const string userId4 = "users/4";
                const string userId5 = "users/5";
                const string userName1 = "Grisha";
                const string userName2 = "Igal";
                const string userName3 = "Egor";
                const string userName4 = "Lev";
                const string userName5 = "Yonatan";

                var deployedIndex = new RelatedUsersIndex();
                await deployedIndex.ExecuteAsync(store);

                using (var session = store.OpenAsyncSession())
                {
                    // users/1 -> users/2
                    await session.StoreAsync(new User {Name = userName1, RelatedUser = userId2}, userId1);

                    // users/4 -> users/5
                    await session.StoreAsync(new User {Name = userName4, RelatedUser = userId5}, userId4);

                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                var index = database.IndexStore.GetIndex(deployedIndex.IndexName);

                using (var session = store.OpenAsyncSession())
                {
                    index.ForTestingPurposesOnly().BeforeClosingDocumentsReadTransactionForHandleReferences = () =>
                    {
                        index.ForTestingPurposesOnly().BeforeClosingDocumentsReadTransactionForHandleReferences = null;

                        using (var session2 = store.OpenSession())
                        {
                            session2.Store(new User {Name = userName3, RelatedUser = null}, userId3);

                            session2.Store(new User {Name = userName5, RelatedUser = null}, userId5);

                            session2.SaveChanges();
                        }
                    };

                    // saving users/2
                    await session.StoreAsync(new User {Name = userName2, RelatedUser = userId3}, userId2);

                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var results = await session
                        .Query<RelatedUsersIndex.Result, RelatedUsersIndex>()
                        .OrderBy(x => x.UserName1)
                        .ProjectInto<RelatedUsersIndex.Result>()
                        .ToListAsync();

                    Assert.Equal(5, results.Count);

                    Assert.Equal(userName3, results[0].UserName1);
                    Assert.Equal(null, results[0].UserName2);
                    Assert.Equal(null, results[0].UserName3);

                    Assert.Equal(userName1, results[1].UserName1);
                    Assert.Equal(userName2, results[1].UserName2);
                    Assert.Equal(userName3, results[1].UserName3);

                    Assert.Equal(userName2, results[2].UserName1);
                    Assert.Equal(userName3, results[2].UserName2);
                    Assert.Equal(null, results[2].UserName3);

                    Assert.Equal(userName4, results[3].UserName1);
                    Assert.Equal(userName5, results[3].UserName2);
                    Assert.Equal(null, results[3].UserName3);

                    Assert.Equal(userName5, results[4].UserName1);
                    Assert.Equal(null, results[4].UserName2);
                    Assert.Equal(null, results[4].UserName3);
                }
            }
        }
    }

    [Fact]
    public async Task Can_Index_Nested_CompareExchange_Change()
    {
        using (var server = GetNewServer())
        using (var store = GetDocumentStore(new Options
               {
                   Server = server
               }))
        {
            const string userId1 = "users/1";
            const string userId2 = "users/2";
            const string userId3 = "users/3";
            const string userId4 = "users/4";
            const string userId5 = "users/5";
            const string userName1 = "Grisha";
            const string userName2 = "Igal";
            const string userName3 = "Egor";
            const string userName4 = "Lev";
            const string userName5 = "Yonatan";

            var deployedIndex = new RelatedUsersCompareExchangeIndex();
            await deployedIndex.ExecuteAsync(store);

            using (var session = store.OpenAsyncSession(new SessionOptions{TransactionMode = TransactionMode.ClusterWide}))
            {
                // users/1 -> users/2
                await session.StoreAsync(new User
                {
                    Name = userName1,
                    RelatedUser = userId2
                }, userId1);

                // users/4 -> users/5
                await session.StoreAsync(new User
                {
                    Name = userName4,
                    RelatedUser = userId5
                }, userId4);

                session.Advanced.WaitForIndexesAfterSaveChanges();
                await session.SaveChangesAsync();
            }

            var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            var index = database.IndexStore.GetIndex(deployedIndex.IndexName);

            using (var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            }))
            {
                index.ForTestingPurposesOnly().BeforeClosingDocumentsReadTransactionForHandleReferences = () =>
                {
                    index.ForTestingPurposesOnly().BeforeClosingDocumentsReadTransactionForHandleReferences = null;

                    using (var session2 = store.OpenSession(new SessionOptions
                           {
                               TransactionMode = TransactionMode.ClusterWide
                           }))
                    {
                        session2.Advanced.ClusterTransaction.CreateCompareExchangeValue(userId3, new User
                        {
                            Name = userName3,
                            RelatedUser = null
                        });

                        session2.Advanced.ClusterTransaction.CreateCompareExchangeValue(userId5, new User
                        {
                            Name = userName5,
                            RelatedUser = null
                        });

                        session2.SaveChanges();
                    }
                };

                // saving users/2
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(userId2, new User
                {
                    Name = userName2,
                    RelatedUser = userId3
                });

                await session.SaveChangesAsync();
            }

            Indexes.WaitForIndexing(store);

            using (var session = store.OpenAsyncSession())
            {
                var results = await session
                    .Query<RelatedUsersCompareExchangeIndex.Result, RelatedUsersCompareExchangeIndex>()
                    .ProjectInto<RelatedUsersCompareExchangeIndex.Result>()
                    .ToListAsync();

                Assert.Equal(2, results.Count);
                Assert.Equal(userName1, results[0].UserName1);
                Assert.Equal(userName2, results[0].UserName2);
                Assert.Equal(userName3, results[0].UserName3);
                Assert.Equal(userName4, results[1].UserName1);
                Assert.Equal(userName5, results[1].UserName2);
                Assert.Equal(null, results[1].UserName3);
            }
        }
    }

    private class User
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public string CompanyId { get; set; }

        public string RelatedUser { get; set; }
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

    private class RelatedUsersIndex : AbstractIndexCreationTask<User>
    {
        public class Result
        {
            public string UserName1 { get; set; }
            public string UserName2 { get; set; }
            public string UserName3 { get; set; }
        }

        public RelatedUsersIndex()
        {
            Map = users =>
                from user in users
                let user2 = LoadDocument<User>(user.RelatedUser)
                let user3 = LoadDocument<User>(user2.RelatedUser)
                select new Result
                {
                    UserName1 = user.Name,
                    UserName2 = user2.Name,
                    UserName3 = user3.Name
                };

            StoreAllFields(FieldStorage.Yes);
        }
    }

    private class RelatedUsersCompareExchangeIndex : AbstractIndexCreationTask<User>
    {
        public class Result
        {
            public string UserName1 { get; set; }
            public string UserName2 { get; set; }
            public string UserName3 { get; set; }
        }

        public RelatedUsersCompareExchangeIndex()
        {
            Map = users =>
                from user in users
                let user2 = LoadCompareExchangeValue<User>(user.RelatedUser)
                let user3 = LoadCompareExchangeValue<User>(user2.RelatedUser)
                select new Result
                {
                    UserName1 = user.Name,
                    UserName2 = user2.Name,
                    UserName3 = user3.Name
                };

            StoreAllFields(FieldStorage.Yes);
        }
    }
}
