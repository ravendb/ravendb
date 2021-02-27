using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Sparrow.Platform;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15754 : RavenTestBase
    {
        private const int _employeesCount = 20_000;
        private const string _managedAllocationsBatchLimit = "16";
        private const string _commonName = "Companies";
        private const string _companyName1 = "Hibernating Rhinos";
        private const string _companyName2 = "HR";

        public RavenDB_15754(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanIndexReferencedDocumentChange()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = _managedAllocationsBatchLimit
            }))
            {
                var company = new Company
                {
                    Name = _companyName1
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

                var index = new DocumentsIndex();
                await new DocumentsIndex().ExecuteAsync(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));
                await AssertCount(store, _companyName1, _employeesCount);

                var batchCount = 0;
                var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

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
                    company.Name = _companyName2;
                    await session.StoreAsync(company, company.Id);
                    await session.SaveChangesAsync();
                }

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(5));
                await AssertCount(store, _companyName1, 0);
                await AssertCount(store, _companyName2, _employeesCount);
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
                var company = new Company
                {
                    Name = _companyName1
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

                var index = new DocumentsIndex();
                await new DocumentsIndex().ExecuteAsync(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));
                await AssertCount(store, _companyName1, _employeesCount);

                var batchCount = 0;
                var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
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
                    company.Name = _companyName2;
                    await session.StoreAsync(company, company.Id);
                    await session.SaveChangesAsync();
                }

                Assert.True(await Task.WhenAny(tcs.Task, Task.Delay(10_000)) == tcs.Task);

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = _companyName1;
                    await session.StoreAsync(company, company.Id);
                    await session.SaveChangesAsync();
                }

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(5));
                await AssertCount(store, _companyName1, _employeesCount);
                await AssertCount(store, _companyName2, 0);

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
                var company = new Company
                {
                    Name = _companyName1
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

                var index = new DocumentsIndex();
                await index.ExecuteAsync(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));
                await AssertCount(store, _companyName1, _employeesCount);

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
                await AssertCount(store, _companyName1, 0);
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
                var company = new Company
                {
                    Name = _companyName1
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

                var index = new DocumentsIndex();
                await index.ExecuteAsync(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));

                await AssertCount(store, _companyName1, _employeesCount);
                await AssertCount(store, _companyName2, 0);

                var batchCount = await AssertBatchCountProgress(store, index.IndexName, GetItemsCount, async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        company.Name = _companyName2;
                        await session.StoreAsync(company, company.Id);
                        await session.SaveChangesAsync();
                    }
                });

                await AssertCount(store, _companyName1, 0);
                await AssertCount(store, _companyName2, _employeesCount);

                Assert.True(batchCount >= 3);

                static async Task<(int CompaniesCount1, int CompaniesCount2)> GetItemsCount(IAsyncDocumentSession session)
                {
                    var query1 = session.Query<DocumentsIndex.Result, DocumentsIndex>()
                        .Where(x => x.CompanyName == _companyName1).CountLazilyAsync();

                    var query2 = session.Query<DocumentsIndex.Result, DocumentsIndex>()
                        .Where(x => x.CompanyName == _companyName2).CountLazilyAsync();

                    return (await query1.Value, await query2.Value);
                }
            }
        }

        [Fact]
        public async Task CanIndexReferencedCompareExchangeWithQuery()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = _managedAllocationsBatchLimit
            }))
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(_commonName, new Company { Name = _companyName1 });
                    await session.SaveChangesAsync();
                }

                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < _employeesCount; i++)
                    {
                        await bulk.StoreAsync(new Employee
                        {
                            CompanyId = _commonName
                        });
                    }
                }

                var index = new DocumentsWithCompareExchangeIndex();
                await index.ExecuteAsync(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));

                await AssertCompaniesCount(store, GetItemsCount, _employeesCount, 0);

                var batchCount = await AssertBatchCountProgress(store, index.IndexName, GetItemsCount, async () =>
                {
                    using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var company = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Company>(_commonName);
                        company.Value.Name = _companyName2;
                        await session.SaveChangesAsync();
                    }
                });

                await AssertCompaniesCount(store, GetItemsCount, 0, _employeesCount);

                Assert.True(batchCount >= 3);

                static async Task<(int CompaniesCount1, int CompaniesCount2)> GetItemsCount(IAsyncDocumentSession session)
                {
                    var query1 = session.Query<DocumentsWithCompareExchangeIndex.Result, DocumentsWithCompareExchangeIndex>()
                        .Where(x => x.CompanyName == _companyName1).CountLazilyAsync();

                    var query2 = session.Query<DocumentsWithCompareExchangeIndex.Result, DocumentsWithCompareExchangeIndex>()
                        .Where(x => x.CompanyName == _companyName2).CountLazilyAsync();

                    return (await query1.Value, await query2.Value);
                }
            }
        }

        [Fact]
        public async Task CanIndexReferencedDocumentByTimeSeriesChangeWithQuery()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = _managedAllocationsBatchLimit
            }))
            {
                var company = new Company
                {
                    Name = _companyName1
                };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();

                    using (var bulk = store.BulkInsert())
                    {
                        var baseDate = DateTime.UtcNow;

                        for (var i = 0; i < _employeesCount; i++)
                        {
                            var employee = new Employee();
                            await bulk.StoreAsync(employee);

                            using (var ts = bulk.TimeSeriesFor(employee.Id, _commonName))
                            {
                                await ts.AppendAsync(baseDate, 1, company.Id);
                            }
                        }
                    }
                }

                var index = new TimeSeriesIndex();
                await index.ExecuteAsync(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));

                await AssertCompaniesCount(store, GetItemsCount, _employeesCount, 0);

                var batchCount = await AssertBatchCountProgress(store, index.IndexName, GetItemsCount, async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        company.Name = _companyName2;
                        await session.StoreAsync(company, company.Id);
                        await session.SaveChangesAsync();
                    }
                });

                await AssertCompaniesCount(store, GetItemsCount, 0, _employeesCount);

                Assert.True(batchCount >= 3);

                static async Task<(int CompaniesCount1, int CompaniesCount2)> GetItemsCount(IAsyncDocumentSession session)
                {
                    var query1 = session.Query<TimeSeriesIndex.Result, TimeSeriesIndex>()
                        .Where(x => x.CompanyName == _companyName1).CountLazilyAsync();

                    var query2 = session.Query<TimeSeriesIndex.Result, TimeSeriesIndex>()
                        .Where(x => x.CompanyName == _companyName2).CountLazilyAsync();

                    return (await query1.Value, await query2.Value);
                }
            }
        }

        [Fact]
        public async Task CanIndexTimeSeriesReferencedCompareExchangeWithQuery()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = _managedAllocationsBatchLimit
            }))
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(_commonName, new Company { Name = _companyName1 });
                    await session.SaveChangesAsync();
                }

                using (var bulk = store.BulkInsert())
                {
                    var baseDate = DateTime.UtcNow;

                    for (var i = 0; i < _employeesCount; i++)
                    {
                        var employee = new Employee();
                        await bulk.StoreAsync(employee);

                        using (var ts = bulk.TimeSeriesFor(employee.Id, _commonName))
                        {
                            await ts.AppendAsync(baseDate, 1, _commonName);
                        }
                    }
                }

                var index = new TimeSeriesWithCompareExchangeIndex();
                await index.ExecuteAsync(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));

                await AssertCompaniesCount(store, GetItemsCount, _employeesCount, 0);

                var batchCount = await AssertBatchCountProgress(store, index.IndexName, GetItemsCount, async () =>
                {
                    using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var company = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Company>(_commonName);
                        company.Value.Name = _companyName2;
                        await session.SaveChangesAsync();
                    }
                });

                await AssertCompaniesCount(store, GetItemsCount, 0, _employeesCount);

                Assert.True(batchCount >= 3);

                static async Task<(int CompaniesCount1, int CompaniesCount2)> GetItemsCount(IAsyncDocumentSession session)
                {
                    var query1 = session.Query<TimeSeriesWithCompareExchangeIndex.Result, TimeSeriesWithCompareExchangeIndex>()
                        .Where(x => x.CompanyName == _companyName1).CountLazilyAsync();

                    var query2 = session.Query<TimeSeriesWithCompareExchangeIndex.Result, TimeSeriesWithCompareExchangeIndex>()
                        .Where(x => x.CompanyName == _companyName2).CountLazilyAsync();

                    return (await query1.Value, await query2.Value);
                }
            }
        }

        [Fact]
        public async Task CanIndexReferencedDocumentByTimeSeriesMapReduceChangeWithQuery()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = _managedAllocationsBatchLimit
            }))
            {
                var company = new Company
                {
                    Name = _companyName1
                };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();

                    using (var bulk = store.BulkInsert())
                    {
                        var baseDate = DateTime.UtcNow;

                        for (var i = 0; i < _employeesCount; i++)
                        {
                            var employee = new Employee();
                            await bulk.StoreAsync(employee);

                            using (var ts = bulk.TimeSeriesFor(employee.Id, _commonName))
                            {
                                await ts.AppendAsync(baseDate, 1, company.Id);
                            }
                        }
                    }
                }

                var index = new TimeSeriesMapReduceIndex();
                await index.ExecuteAsync(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));

                await AssertCompaniesCount(store, GetItemsCount, _employeesCount, 0);

                var batchCount = await AssertBatchCountProgress(store, index.IndexName, GetItemsCount, async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        company.Name = _companyName2;
                        await session.StoreAsync(company, company.Id);
                        await session.SaveChangesAsync();
                    }
                });

                await AssertCompaniesCount(store, GetItemsCount, 0, _employeesCount);

                Assert.True(batchCount >= 3);

                static async Task<(int CompaniesCount1, int CompaniesCount2)> GetItemsCount(IAsyncDocumentSession session)
                {
                    var query1 = session.Query<TimeSeriesMapReduceIndex.Result, TimeSeriesMapReduceIndex>()
                        .Where(x => x.CompanyName == _companyName1).CountLazilyAsync();

                    var query2 = session.Query<TimeSeriesMapReduceIndex.Result, TimeSeriesMapReduceIndex>()
                        .Where(x => x.CompanyName == _companyName2).CountLazilyAsync();

                    return (await query1.Value, await query2.Value);
                }
            }
        }

        [Fact]
        public async Task CanIndexReferencedDocumentByCountersChangeWithQuery()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = _managedAllocationsBatchLimit
            }))
            {
                var company = new Company
                {
                    Id = _commonName,
                    Name = _companyName1
                };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();

                    using (var bulk = store.BulkInsert())
                    {
                        for (var i = 0; i < _employeesCount; i++)
                        {
                            var employee = new Employee();
                            await bulk.StoreAsync(employee);
                            await bulk.CountersFor(employee.Id).IncrementAsync(_commonName);
                        }
                    }
                }

                var index = new CountersIndex();
                await index.ExecuteAsync(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));

                await AssertCompaniesCount(store, GetItemsCount, _employeesCount, 0);

                var batchCount = await AssertBatchCountProgress(store, index.IndexName, GetItemsCount, async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        company.Name = _companyName2;
                        await session.StoreAsync(company, company.Id);
                        await session.SaveChangesAsync();
                    }
                });

                await AssertCompaniesCount(store, GetItemsCount, 0, _employeesCount);

                Assert.True(batchCount >= 3);

                static async Task<(int CompaniesCount1, int CompaniesCount2)> GetItemsCount(IAsyncDocumentSession session)
                {
                    var query1 = session.Query<CountersIndex.Result, CountersIndex>()
                        .Where(x => x.CompanyName == _companyName1).CountLazilyAsync();

                    var query2 = session.Query<CountersIndex.Result, CountersIndex>()
                        .Where(x => x.CompanyName == _companyName2).CountLazilyAsync();

                    return (await query1.Value, await query2.Value);
                }
            }
        }

        [Fact]
        public async Task CanIndexCountersReferencedCompareExchangeWithQuery()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = _managedAllocationsBatchLimit
            }))
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(_commonName, new Company { Name = _companyName1 });
                    await session.SaveChangesAsync();
                }

                using (var bulk = store.BulkInsert())
                {
                    for (var i = 0; i < _employeesCount; i++)
                    {
                        var employee = new Employee();
                        await bulk.StoreAsync(employee);
                        await bulk.CountersFor(employee.Id).IncrementAsync(_commonName);
                    }
                }

                var index = new CountersWithCompareExchangeIndex();
                await index.ExecuteAsync(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));

                await AssertCompaniesCount(store, GetItemsCount, _employeesCount, 0);

                var batchCount = await AssertBatchCountProgress(store, index.IndexName, GetItemsCount, async () =>
                {
                    using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var company = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Company>(_commonName);
                        company.Value.Name = _companyName2;
                        await session.SaveChangesAsync();
                    }
                });

                await AssertCompaniesCount(store, GetItemsCount, 0, _employeesCount);

                Assert.True(batchCount >= 3);

                static async Task<(int CompaniesCount1, int CompaniesCount2)> GetItemsCount(IAsyncDocumentSession session)
                {
                    var query1 = session.Query<CountersWithCompareExchangeIndex.Result, CountersWithCompareExchangeIndex>()
                        .Where(x => x.CompanyName == _companyName1).CountLazilyAsync();

                    var query2 = session.Query<CountersWithCompareExchangeIndex.Result, CountersWithCompareExchangeIndex>()
                        .Where(x => x.CompanyName == _companyName2).CountLazilyAsync();

                    return (await query1.Value, await query2.Value);
                }
            }
        }

        [Fact]
        public async Task CanIndexReferencedDocumentByCountersMapReduceChangeWithQuery()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = _managedAllocationsBatchLimit
            }))
            {
                var company = new Company
                {
                    Id = _commonName,
                    Name = _companyName1
                };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();

                    using (var bulk = store.BulkInsert())
                    {
                        for (var i = 0; i < _employeesCount; i++)
                        {
                            var employee = new Employee();
                            await bulk.StoreAsync(employee);
                            await bulk.CountersFor(employee.Id).IncrementAsync(_commonName);
                        }
                    }
                }

                var index = new MapReduceCountersIndex();
                await index.ExecuteAsync(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));

                await AssertCompaniesCount(store, GetItemsCount, _employeesCount, 0);

                var batchCount = await AssertBatchCountProgress(store, index.IndexName, GetItemsCount, async () =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        company.Name = _companyName2;
                        await session.StoreAsync(company, company.Id);
                        await session.SaveChangesAsync();
                    }
                });

                await AssertCompaniesCount(store, GetItemsCount, 0, _employeesCount);

                Assert.True(batchCount >= 3);

                static async Task<(int CompaniesCount1, int CompaniesCount2)> GetItemsCount(IAsyncDocumentSession session)
                {
                    var query1 = session.Query<MapReduceCountersIndex.Result, MapReduceCountersIndex>()
                        .Where(x => x.CompanyName == _companyName1).CountLazilyAsync();

                    var query2 = session.Query<MapReduceCountersIndex.Result, MapReduceCountersIndex>()
                        .Where(x => x.CompanyName == _companyName2).CountLazilyAsync();

                    await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync();

                    return (await query1.Value, await query2.Value);
                }
            }
        }

        private static async Task AssertCount(DocumentStore store, string companyName, int expectedCount)
        {
            using (var session = store.OpenAsyncSession())
            {
                var itemsCount = await session.Query<DocumentsIndex.Result, DocumentsIndex>()
                    .Where(x => x.CompanyName == companyName).CountAsync();

                Assert.Equal(expectedCount, itemsCount);
            }
        }

        private static async Task AssertCompaniesCount(DocumentStore store, Func<IAsyncDocumentSession, Task<(int CompaniesCount1, int CompaniesCount2)>> getItemsCount, int expectedCount1, int expectedCount2)
        {
            using (var session = store.OpenAsyncSession())
            {
                var itemsCount = await getItemsCount(session);
                Assert.Equal(expectedCount1, itemsCount.CompaniesCount1);
                Assert.Equal(expectedCount2, itemsCount.CompaniesCount2);
            }
        }

        private async Task<int> AssertBatchCountProgress(IDocumentStore store, string indexName, Func<IAsyncDocumentSession, Task<(int CompaniesCount1, int CompaniesCount2)>> getItemsCount, Func<Task> modifyItem)
        {
            var batchCount = 0;

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.MaxNumberOfRequestsPerSession = PlatformDetails.Is32Bits == false ? 50 : 200;

                (int itemsCount1, int itemsCount2) = await getItemsCount(session);
                Assert.Equal(_employeesCount, itemsCount1);
                Assert.Equal(0, itemsCount2);

                // stop indexing before modifying the item
                await store.Maintenance.SendAsync(new StopIndexingOperation());

                await modifyItem();

                var documentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                var index = documentDatabase.IndexStore.GetIndex(indexName);
                var prop = typeof(Raven.Server.Documents.Indexes.Index).GetField("_indexDisabled", System.Reflection.BindingFlags.NonPublic
                                                                                                  | System.Reflection.BindingFlags.Instance);
                prop.SetValue(index, false);

                var firstRunStats = new IndexingRunStats();
                var scope = new IndexingStatsScope(firstRunStats);
                while (index.DoIndexingWork(scope, CancellationToken.None))
                {
                    batchCount++;

                    (int newItemsCount1, int newItemsCount2) = await getItemsCount(session);
                    Assert.True(itemsCount1 > newItemsCount1,
                        $"new count1: {newItemsCount1}, old count1: {itemsCount1}, iteration: {batchCount}");
                    Assert.True(itemsCount2 < newItemsCount2,
                        $"new count2: {newItemsCount2}, old count2: {itemsCount2}, iteration: {batchCount}");

                    Assert.Equal(_employeesCount, newItemsCount1 + newItemsCount2);

                    itemsCount1 = newItemsCount1;
                    itemsCount2 = newItemsCount2;
                }
            }

            return batchCount;
        }

        private class Company
        {
            public string Id { get; set; }

            public string Name { get; set; }
        }

        private class Employee
        {
            public string Id { get; set; }

            public string CompanyId { get; set; }
        }

        private class DocumentsIndex : AbstractIndexCreationTask<Employee>
        {
            public class Result
            {
                public string CompanyName { get; set; }
            }

            public DocumentsIndex()
            {
                Map = employees =>
                    from employee in employees
                    select new Result
                    {
                        CompanyName = LoadDocument<Company>(employee.CompanyId).Name
                    };
            }
        }

        private class DocumentsWithCompareExchangeIndex : AbstractIndexCreationTask<Employee>
        {
            public class Result
            {
                public string CompanyName { get; set; }
            }

            public DocumentsWithCompareExchangeIndex()
            {
                Map = employees =>
                    from employee in employees
                    select new Result
                    {
                        CompanyName = LoadCompareExchangeValue<Company>(employee.CompanyId).Name
                    };
            }
        }

        private class TimeSeriesIndex : AbstractTimeSeriesIndexCreationTask<Employee>
        {
            public class Result
            {
                public string CompanyName { get; set; }
            }

            public TimeSeriesIndex()
            {
                AddMap(
                    _commonName,
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  select new Result
                                  {
                                      CompanyName = LoadDocument<Company>(entry.Tag).Name
                                  });
            }
        }

        private class TimeSeriesMapReduceIndex : AbstractTimeSeriesIndexCreationTask<Employee, TimeSeriesMapReduceIndex.Result>
        {
            public class Result
            {
                public string DocumentId { get; set; }

                public string CompanyName { get; set; }
            }

            public TimeSeriesMapReduceIndex()
            {
                AddMap(
                    _commonName,
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  select new Result
                                  {
                                      DocumentId = ts.DocumentId,
                                      CompanyName = LoadDocument<Company>(entry.Tag).Name
                                  });

                Reduce = results => from r in results
                                    group r by r.DocumentId into g
                                    select new Result
                                    {
                                        DocumentId = g.Key,
                                        CompanyName = g.Select(x => x.CompanyName).FirstOrDefault()
                                    };
            }
        }

        private class TimeSeriesWithCompareExchangeIndex : AbstractTimeSeriesIndexCreationTask<Employee>
        {
            public class Result
            {
                public string CompanyName { get; set; }
            }

            public TimeSeriesWithCompareExchangeIndex()
            {
                AddMap(
                    _commonName,
                    timeSeries =>
                        from ts in timeSeries
                        from entry in ts.Entries
                        select new Result
                        {
                            CompanyName = LoadCompareExchangeValue<Company>(entry.Tag).Name
                        });
            }
        }

        private class CountersIndex : AbstractCountersIndexCreationTask<Employee>
        {
            public class Result
            {
                public string CompanyName { get; set; }
            }

            public CountersIndex()
            {
                AddMap(_commonName,
                    counters => from counter in counters
                                select new Result
                                {
                                    CompanyName = LoadDocument<Company>(counter.Name).Name
                                });
            }
        }

        private class MapReduceCountersIndex : AbstractCountersIndexCreationTask<Employee, MapReduceCountersIndex.Result>
        {
            public class Result
            {
                public string DocumentId { get; set; }

                public string CompanyName { get; set; }
            }

            public MapReduceCountersIndex()
            {
                AddMap(_commonName,
                    counters => from counter in counters
                                select new Result
                                {
                                    DocumentId = counter.DocumentId,
                                    CompanyName = LoadDocument<Company>(counter.Name).Name
                                });

                Reduce = results => from r in results
                                    group r by r.DocumentId into g
                                    select new Result
                                    {
                                        DocumentId = g.Key,
                                        CompanyName = g.Select(x => x.CompanyName).FirstOrDefault()
                                    };
            }
        }

        private class CountersWithCompareExchangeIndex : AbstractCountersIndexCreationTask<Employee>
        {
            public class Result
            {
                public string CompanyName { get; set; }
            }

            public CountersWithCompareExchangeIndex()
            {
                AddMap(
                    _commonName,
                    counters =>
                        from counter in counters
                        select new Result
                        {
                            CompanyName = LoadCompareExchangeValue<Company>(counter.Name).Name
                        });
            }
        }
    }
}
