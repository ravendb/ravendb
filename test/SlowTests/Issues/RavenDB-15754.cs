using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15754 : RavenTestBase
    {
        public RavenDB_15754(ITestOutputHelper output) : base(output)
        {
        }

        private const int _employeesCount = 20_000;
        private const string _managedAllocationsBatchLimit = "16";

        [Fact]
        public async Task CanIndexReferencedDocumentChange()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = _managedAllocationsBatchLimit
            }))
            {
                const string companyName1 = "Hibernating Rhinos";
                const string companyName2 = "HR";
                var company = new Company
                {
                    Name = companyName1
                };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();

                    using (var bulk = store.BulkInsert())
                    {
                        for (var i = 0; i < _employeesCount; i++)
                        {
                            await bulk.StoreAsync(new Employee
                            {
                                CompanyId = company.Id
                            });
                        }
                    }
                }

                var index = new Index();
                await new Index().ExecuteAsync(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));
                await AssertCount(store, companyName1, _employeesCount);

                var batchCount = 0;
                var tcs = new TaskCompletionSource<object>();
                
                store.Changes().ForIndex(index.IndexName).Subscribe(x =>
                {
                    if (x.Type == IndexChangeTypes.BatchCompleted)
                    {
                        if(Interlocked.Increment(ref batchCount) > 1)
                            tcs.SetResult(null);
                    }
                });

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = companyName2;
                    await session.StoreAsync(company, company.Id);
                    await session.SaveChangesAsync();
                }

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(5));
                await AssertCount(store, companyName1, 0);
                await AssertCount(store, companyName2, _employeesCount);
                Assert.True(await Task.WhenAny(tcs.Task, Task.Delay(10_000)) == tcs.Task);
                Assert.True(batchCount > 1);
            }
        }

        [Fact]
        public async Task CanIndexReferencedDocumentMultipleChanges()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = _managedAllocationsBatchLimit
            }))
            {
                const string companyName1 = "Hibernating Rhinos";
                const string companyName2 = "HR";
                var company = new Company
                {
                    Name = companyName1
                };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();

                    using (var bulk = store.BulkInsert())
                    {
                        for (var i = 0; i < _employeesCount; i++)
                        {
                            await bulk.StoreAsync(new Employee
                            {
                                CompanyId = company.Id
                            });
                        }
                    }
                }

                var index = new Index();
                await new Index().ExecuteAsync(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));
                await AssertCount(store, companyName1, _employeesCount);

                var batchCount = 0;
                var tcs = new TaskCompletionSource<object>();
                store.Changes().ForIndex(index.IndexName).Subscribe(x =>
                {
                    if (x.Type == IndexChangeTypes.BatchCompleted)
                    {
                        if (Interlocked.Increment(ref batchCount) > 1)
                            tcs.TrySetResult(null);
                    }
                });

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = companyName2;
                    await session.StoreAsync(company, company.Id);
                    await session.SaveChangesAsync();
                }

                Assert.True(await Task.WhenAny(tcs.Task, Task.Delay(10_000)) == tcs.Task);

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = companyName1;
                    await session.StoreAsync(company, company.Id);
                    await session.SaveChangesAsync();
                }

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(5));
                await AssertCount(store, companyName1, _employeesCount);
                await AssertCount(store, companyName2, 0);

                Assert.True(batchCount > 1);
            }
        }

        [Fact]
        public async Task CanIndexReferencedDocumentDelete()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = _managedAllocationsBatchLimit
            }))
            {
                const string companyName1 = "Hibernating Rhinos";
                var company = new Company
                {
                    Name = companyName1
                };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();

                    using (var bulk = store.BulkInsert())
                    {
                        for (var i = 0; i < _employeesCount; i++)
                        {
                            await bulk.StoreAsync(new Employee
                            {
                                CompanyId = company.Id
                            });
                        }
                    }
                }

                var index = new Index();
                await index.ExecuteAsync(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));
                await AssertCount(store, companyName1, _employeesCount);

                var batchCount = 0;
                var tcs = new TaskCompletionSource<object>();
                store.Changes().ForIndex(index.IndexName).Subscribe(x =>
                {
                    if (x.Type == IndexChangeTypes.BatchCompleted)
                    {
                        if (Interlocked.Increment(ref batchCount) > 1)
                            tcs.TrySetResult(null);
                    }
                });

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(company.Id);
                    await session.SaveChangesAsync();
                }

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(5));
                await AssertCount(store, companyName1, 0);
                Assert.True(await Task.WhenAny(tcs.Task, Task.Delay(10_000)) == tcs.Task);

                Assert.True(batchCount > 1);
            }
        }

        [Fact]
        public async Task CanIndexReferencedDocumentChangeWithQuery()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = _managedAllocationsBatchLimit
            }))
            {
                const string companyName1 = "Hibernating Rhinos";
                const string companyName2 = "HR";
                var company = new Company
                {
                    Name = companyName1
                };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();

                    using (var bulk = store.BulkInsert())
                    {
                        for (var i = 0; i < _employeesCount; i++)
                        {
                            await bulk.StoreAsync(new Employee
                            {
                                CompanyId = company.Id
                            });
                        }
                    }
                }

                var index = new Index();
                await new Index().ExecuteAsync(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));
                await AssertCount(store, companyName1, _employeesCount);
                await AssertCount(store, companyName2, 0);

                var batchCount = 0;
                var tcs = new TaskCompletionSource<object>();

                store.Changes().ForIndex(index.IndexName).Subscribe(x =>
                {
                    if (x.Type == IndexChangeTypes.BatchCompleted)
                    {
                        if (Interlocked.Increment(ref batchCount) > 1)
                            tcs.SetResult(null);
                    }
                });

                using (var session = store.OpenAsyncSession())
                {
                    var itemsCount1 = await GetItemsCount(session, companyName1);
                    Assert.Equal(_employeesCount, itemsCount1);

                    var itemsCount2 = await GetItemsCount(session, companyName2);
                    Assert.Equal(0, itemsCount2);

                    using (var internalSession = store.OpenAsyncSession())
                    {
                        company.Name = companyName2;
                        await internalSession.StoreAsync(company, company.Id);
                        await internalSession.SaveChangesAsync();
                    }

                    while (itemsCount1 > 0 || itemsCount2 != _employeesCount)
                    {
                        // wait for the batch to complete
                        Assert.True(await Task.WhenAny(tcs.Task, Task.Delay(10_000)) == tcs.Task);
                        tcs = new TaskCompletionSource<object>();

                        var newItemsCount1 = await GetItemsCount(session, companyName1);
                        Assert.True(newItemsCount1 == 0 || (newItemsCount1 > 0 && itemsCount1 != newItemsCount1));

                        var newItemsCount2 = await GetItemsCount(session, companyName2);
                        Assert.True(newItemsCount2 == _employeesCount || (newItemsCount2 > 0 && itemsCount2 != newItemsCount2));

                        itemsCount1 = newItemsCount1;
                        itemsCount2 = newItemsCount2;
                    }
                }

                await AssertCount(store, companyName1, 0);
                await AssertCount(store, companyName2, _employeesCount);
                
                Assert.True(batchCount > 1);

                static async Task<int> GetItemsCount(IAsyncDocumentSession session, string companyName)
                {
                    return await session.Query<Index.Result, Index>()
                        .Where(x => x.CompanyName == companyName).CountAsync();
                }
            }
        }

        private static async Task AssertCount(DocumentStore store, string companyName, int expectedCount)
        {
            using (var session = store.OpenAsyncSession())
            {
                var itemsCount = await session.Query<Index.Result, Index>()
                    .Where(x => x.CompanyName == companyName).CountAsync();

                Assert.Equal(expectedCount, itemsCount);
            }
        }

        private class Company
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }

        private class Employee
        {
            public string CompanyId { get; set; }
        }

        private class Index : AbstractIndexCreationTask<Employee>
        {
            public class Result
            {
                public string CompanyName { get; set; }
            }

            public Index()
            {
                Map = employees =>
                    from employee in employees
                    select new Result
                    {
                        CompanyName = LoadDocument<Company>(employee.CompanyId).Name
                    };
            }
        }
    }
}
