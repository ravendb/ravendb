using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
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
                new Index().Execute(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));
                await AssertCount(store, companyName1, _employeesCount);

                var batchCount = 0;
                store.Changes().ForIndex(index.IndexName).Subscribe(x =>
                {
                    if (x.Type == IndexChangeTypes.BatchCompleted)
                    {
                        batchCount++;
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

                Assert.True(batchCount >= 5, batchCount.ToString());
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
                new Index().Execute(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));
                await AssertCount(store, companyName1, _employeesCount);

                var batchCount = 0;
                var sm = new SemaphoreSlim(0);
                store.Changes().ForIndex(index.IndexName).Subscribe(x =>
                {
                    if (x.Type == IndexChangeTypes.BatchCompleted)
                    {
                        sm.Release();
                        batchCount++;
                    }
                });

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = companyName2;
                    await session.StoreAsync(company, company.Id);
                    await session.SaveChangesAsync();
                }

                sm.Wait();

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = companyName1;
                    await session.StoreAsync(company, company.Id);
                    await session.SaveChangesAsync();
                }

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(5));
                await AssertCount(store, companyName1, _employeesCount);
                await AssertCount(store, companyName2, 0);

                Assert.True(batchCount >= 5);
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
                index.Execute(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));
                await AssertCount(store, companyName1, _employeesCount);

                var batchCount = 0;
                store.Changes().ForIndex(index.IndexName).Subscribe(x =>
                {
                    if (x.Type == IndexChangeTypes.BatchCompleted)
                    {
                        batchCount++;
                    }
                });

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(company.Id);
                    await session.SaveChangesAsync();
                }

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(5));
                await AssertCount(store, companyName1, 0);

                Assert.True(batchCount >= 5);
            }
        }

        private async Task AssertCount(DocumentStore store, string companyName, int expectedCount)
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
