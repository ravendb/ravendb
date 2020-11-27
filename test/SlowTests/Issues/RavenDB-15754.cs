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
using Raven.Client.Documents.Session;
using Raven.Client.Util;
using Raven.Server.Config;
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
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
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
                    await session.SaveChangesAsync(cts.Token);

                    using (var bulk = store.BulkInsert(token: cts.Token))
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
                await AssertCount(store, _companyName1, _employeesCount, cts.Token);

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
                    company.Name = _companyName2;
                    await session.StoreAsync(company, company.Id, cts.Token);
                    await session.SaveChangesAsync(cts.Token);
                }

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(5));
                await AssertCount(store, _companyName1, 0, cts.Token);
                await AssertCount(store, _companyName2, _employeesCount, cts.Token);
                Assert.True(await Task.WhenAny(tcs.Task, Task.Delay(10_000)) == tcs.Task);
                Assert.True(batchCount > 1);
            }
        }

        [Fact]
        public async Task CanIndexReferencedDocumentMultipleChanges()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
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
                    await session.SaveChangesAsync(cts.Token);

                    using (var bulk = store.BulkInsert(token: cts.Token))
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
                await new DocumentsIndex().ExecuteAsync(store, token: cts.Token);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));
                await AssertCount(store, _companyName1, _employeesCount, cts.Token);

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
                    company.Name = _companyName2;
                    await session.StoreAsync(company, company.Id);
                    await session.SaveChangesAsync(cts.Token);
                }

                Assert.True(await Task.WhenAny(tcs.Task, Task.Delay(10_000)) == tcs.Task);

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = _companyName1;
                    await session.StoreAsync(company, company.Id);
                    await session.SaveChangesAsync(cts.Token);
                }

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(5));
                await AssertCount(store, _companyName1, _employeesCount, cts.Token);
                await AssertCount(store, _companyName2, 0, cts.Token);

                Assert.True(batchCount > 1);
            }
        }

        [Fact]
        public async Task CanIndexReferencedDocumentDelete()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
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
                    await session.SaveChangesAsync(cts.Token);

                    using (var bulk = store.BulkInsert(token: cts.Token))
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
                await AssertCount(store, _companyName1, _employeesCount, cts.Token);

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
                    await session.SaveChangesAsync(cts.Token);
                }

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(5));
                await AssertCount(store, _companyName1, 0, cts.Token);
                Assert.True(await Task.WhenAny(tcs.Task, Task.Delay(10_000)) == tcs.Task);

                Assert.True(batchCount > 1);
            }
        }

        [Fact]
        public async Task CanIndexReferencedDocumentChangeWithQuery()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
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
                    await session.SaveChangesAsync(cts.Token);

                    using (var bulk = store.BulkInsert(token: cts.Token))
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
                await index.ExecuteAsync(store, token: cts.Token);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));

                await AssertCount(store, _companyName1, _employeesCount, cts.Token);
                await AssertCount(store, _companyName2, 0, cts.Token);

                var batchCount = await AssertBatchCountProgress(store, index.IndexName, GetItemsCount, async token =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        company.Name = _companyName2;
                        await session.StoreAsync(company, company.Id);
                        await session.SaveChangesAsync(token);
                    }
                }, cts.Token);

                await AssertCount(store, _companyName1, 0, cts.Token);
                await AssertCount(store, _companyName2, _employeesCount, cts.Token);

                Assert.True(batchCount > 1);

                static async Task<int> GetItemsCount(IAsyncDocumentSession session, string companyName, CancellationToken token)
                {
                    return await session
                        .Query<DocumentsIndex.Result, DocumentsIndex>()
                        .Where(x => x.CompanyName == companyName)
                        .CountAsync(token);
                }
            }
        }

        [Fact]
        public async Task CanIndexReferencedCompareExchangeWithQuery()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = _managedAllocationsBatchLimit
            }))
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(_commonName, new Company { Name = _companyName1 });
                    await session.SaveChangesAsync(cts.Token);
                }

                using (var bulk = store.BulkInsert(token: cts.Token))
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

                await AssertDocumentsWithCompareExchangeCount(store, _companyName1, _employeesCount, cts.Token);
                await AssertDocumentsWithCompareExchangeCount(store, _companyName2, 0, cts.Token);

                var batchCount = await AssertBatchCountProgress(store, index.IndexName, GetItemsCount, async token =>
                {
                    using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var company = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Company>(_commonName);
                        company.Value.Name = _companyName2;
                        await session.SaveChangesAsync(token);
                    }
                }, cts.Token);

                await AssertDocumentsWithCompareExchangeCount(store, _companyName1, 0, cts.Token);
                await AssertDocumentsWithCompareExchangeCount(store, _companyName2, _employeesCount, cts.Token);

                Assert.True(batchCount > 1);

                static async Task AssertDocumentsWithCompareExchangeCount(DocumentStore store, string companyName, int expectedCount, CancellationToken token)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var itemsCount = await GetItemsCount(session, companyName, token);
                        Assert.Equal(expectedCount, itemsCount);
                    }
                }

                static async Task<int> GetItemsCount(IAsyncDocumentSession session, string companyName, CancellationToken token)
                {
                    return await session
                        .Query<DocumentsWithCompareExchangeIndex.Result, DocumentsWithCompareExchangeIndex>()
                        .Where(x => x.CompanyName == companyName)
                        .CountAsync(token);
                }
            }
        }

        [Fact]
        public async Task CanIndexReferencedDocumentByTimeSeriesChangeWithQuery()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
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
                    await session.SaveChangesAsync(cts.Token);

                    using (var bulk = store.BulkInsert(token: cts.Token))
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

                await AssertTimeSeriesCount(store, _companyName1, _employeesCount, cts.Token);
                await AssertTimeSeriesCount(store, _companyName2, 0, cts.Token);

                var batchCount = await AssertBatchCountProgress(store, index.IndexName, GetItemsCount, async token =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        company.Name = _companyName2;
                        await session.StoreAsync(company, company.Id);
                        await session.SaveChangesAsync();
                    }
                }, cts.Token);

                await AssertTimeSeriesCount(store, _companyName1, 0, cts.Token);
                await AssertTimeSeriesCount(store, _companyName2, _employeesCount, cts.Token);

                Assert.True(batchCount > 1);

                static async Task AssertTimeSeriesCount(DocumentStore store, string companyName, int expectedCount, CancellationToken token)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var itemsCount = await GetItemsCount(session, companyName, token);
                        Assert.Equal(expectedCount, itemsCount);
                    }
                }

                static async Task<int> GetItemsCount(IAsyncDocumentSession session, string companyName, CancellationToken token)
                {
                    return await session
                        .Query<TimeSeriesIndex.Result, TimeSeriesIndex>()
                        .Where(x => x.CompanyName == companyName)
                        .CountAsync(token);
                }
            }
        }

        [Fact]
        public async Task CanIndexTimeSeriesReferencedCompareExchangeWithQuery()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = _managedAllocationsBatchLimit
            }))
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(_commonName, new Company { Name = _companyName1 });
                    await session.SaveChangesAsync(cts.Token);
                }

                using (var bulk = store.BulkInsert(token: cts.Token))
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

                await AssertTimeSeriesCount(store, _companyName1, _employeesCount, cts.Token);
                await AssertTimeSeriesCount(store, _companyName2, 0, cts.Token);

                var batchCount = await AssertBatchCountProgress(store, index.IndexName, GetItemsCount, async token =>
                {
                    using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var company = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Company>(_commonName);
                        company.Value.Name = _companyName2;
                        await session.SaveChangesAsync(token);
                    }
                }, cts.Token);

                await AssertTimeSeriesCount(store, _companyName1, 0, cts.Token);
                await AssertTimeSeriesCount(store, _companyName2, _employeesCount, cts.Token);

                Assert.True(batchCount > 1);

                static async Task AssertTimeSeriesCount(DocumentStore store, string companyName, int expectedCount, CancellationToken token)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var itemsCount = await GetItemsCount(session, companyName, token);
                        Assert.Equal(expectedCount, itemsCount);
                    }
                }

                static async Task<int> GetItemsCount(IAsyncDocumentSession session, string companyName, CancellationToken token)
                {
                    return await session
                        .Query<TimeSeriesWithCompareExchangeIndex.Result, TimeSeriesWithCompareExchangeIndex>()
                        .Where(x => x.CompanyName == companyName)
                        .CountAsync(token);
                }
            }
        }

        [Fact]
        public async Task CanIndexReferencedDocumentByTimeSeriesMapReduceChangeWithQuery()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
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
                    await session.SaveChangesAsync(cts.Token);

                    using (var bulk = store.BulkInsert(token: cts.Token))
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

                await AssertTimeSeriesCount(store, _companyName1, _employeesCount, cts.Token);
                await AssertTimeSeriesCount(store, _companyName2, 0, cts.Token);

                var batchCount = await AssertBatchCountProgress(store, index.IndexName, GetItemsCount, async token =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        company.Name = _companyName2;
                        await session.StoreAsync(company, company.Id);
                        await session.SaveChangesAsync(token);
                    }
                }, cts.Token);

                await AssertTimeSeriesCount(store, _companyName1, 0, cts.Token);
                await AssertTimeSeriesCount(store, _companyName2, _employeesCount, cts.Token);

                Assert.True(batchCount > 1);

                static async Task AssertTimeSeriesCount(DocumentStore store, string companyName, int expectedCount, CancellationToken token)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var itemsCount = await GetItemsCount(session, companyName, token);
                        Assert.Equal(expectedCount, itemsCount);
                    }
                }

                static async Task<int> GetItemsCount(IAsyncDocumentSession session, string companyName, CancellationToken token)
                {
                    return await session
                        .Query<TimeSeriesMapReduceIndex.Result, TimeSeriesMapReduceIndex>()
                        .Where(x => x.CompanyName == companyName)
                        .CountAsync(token);
                }
            }
        }

        [Fact]
        public async Task CanIndexReferencedDocumentByCountersChangeWithQuery()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
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
                    await session.SaveChangesAsync(cts.Token);

                    using (var bulk = store.BulkInsert(token: cts.Token))
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
                await index.ExecuteAsync(store, token: cts.Token);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));

                await AssertCountersCount(store, _companyName1, _employeesCount, cts.Token);
                await AssertCountersCount(store, _companyName2, 0, cts.Token);

                var batchCount = await AssertBatchCountProgress(store, index.IndexName, GetItemsCount, async token =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        company.Name = _companyName2;
                        await session.StoreAsync(company, company.Id);
                        await session.SaveChangesAsync(token);
                    }
                }, cts.Token);

                await AssertCountersCount(store, _companyName1, 0, cts.Token);
                await AssertCountersCount(store, _companyName2, _employeesCount, cts.Token);

                Assert.True(batchCount > 1);

                static async Task AssertCountersCount(DocumentStore store, string companyName, int expectedCount, CancellationToken token)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var itemsCount = await GetItemsCount(session, companyName, token);
                        Assert.Equal(expectedCount, itemsCount);
                    }
                }

                static async Task<int> GetItemsCount(IAsyncDocumentSession session, string companyName, CancellationToken token)
                {
                    return await session
                        .Query<CountersIndex.Result, CountersIndex>()
                        .Where(x => x.CompanyName == companyName)
                        .CountAsync(token);
                }
            }
        }

        [Fact]
        public async Task CanIndexCountersReferencedCompareExchangeWithQuery()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = x => x.Settings[RavenConfiguration.GetKey(x => x.Indexing.ManagedAllocationsBatchLimit)] = _managedAllocationsBatchLimit
            }))
            {
                using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue(_commonName, new Company { Name = _companyName1 });
                    await session.SaveChangesAsync(cts.Token);
                }

                using (var bulk = store.BulkInsert(token: cts.Token))
                {
                    for (var i = 0; i < _employeesCount; i++)
                    {
                        var employee = new Employee();
                        await bulk.StoreAsync(employee);
                        await bulk.CountersFor(employee.Id).IncrementAsync(_commonName);
                    }
                }

                var index = new CountersWithCompareExchangeIndex();
                await index.ExecuteAsync(store, token: cts.Token);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));

                await AssertCountersCount(store, _companyName1, _employeesCount, cts.Token);
                await AssertCountersCount(store, _companyName2, 0, cts.Token);

                var batchCount = await AssertBatchCountProgress(store, index.IndexName, GetItemsCount, async token =>
                {
                    using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var company = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Company>(_commonName);
                        company.Value.Name = _companyName2;
                        await session.SaveChangesAsync(token);
                    }
                }, cts.Token);

                await AssertCountersCount(store, _companyName1, 0, cts.Token);
                await AssertCountersCount(store, _companyName2, _employeesCount, cts.Token);

                Assert.True(batchCount > 1);

                static async Task AssertCountersCount(DocumentStore store, string companyName, int expectedCount, CancellationToken token)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var itemsCount = await GetItemsCount(session, companyName, token);
                        Assert.Equal(expectedCount, itemsCount);
                    }
                }

                static async Task<int> GetItemsCount(IAsyncDocumentSession session, string companyName, CancellationToken token)
                {
                    return await session
                        .Query<CountersWithCompareExchangeIndex.Result, CountersWithCompareExchangeIndex>()
                        .Where(x => x.CompanyName == companyName)
                        .CountAsync(token);
                }
            }
        }

        [Fact]
        public async Task CanIndexReferencedDocumentByCountersMapReduceChangeWithQuery()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
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
                    await session.StoreAsync(company, cts.Token);
                    await session.SaveChangesAsync(cts.Token);

                    using (var bulk = store.BulkInsert(token: cts.Token))
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
                await index.ExecuteAsync(store, token: cts.Token);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));

                await AssertCountersCount(store, _companyName1, _employeesCount, cts.Token);
                await AssertCountersCount(store, _companyName2, 0, cts.Token);

                var batchCount = await AssertBatchCountProgress(store, index.IndexName, GetItemsCount, async token =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        company.Name = _companyName2;
                        await session.StoreAsync(company, company.Id);
                        await session.SaveChangesAsync(token);
                    }
                }, cts.Token);

                await AssertCountersCount(store, _companyName1, 0, cts.Token);
                await AssertCountersCount(store, _companyName2, _employeesCount, cts.Token);

                Assert.True(batchCount > 1);

                static async Task AssertCountersCount(DocumentStore store, string companyName, int expectedCount, CancellationToken token)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var itemsCount = await GetItemsCount(session, companyName, token);
                        Assert.Equal(expectedCount, itemsCount);
                    }
                }

                static async Task<int> GetItemsCount(IAsyncDocumentSession session, string companyName, CancellationToken token)
                {
                    return await session
                        .Query<MapReduceCountersIndex.Result, MapReduceCountersIndex>()
                        .Where(x => x.CompanyName == companyName)
                        .CountAsync(token);
                }
            }
        }

        private static async Task AssertCount(DocumentStore store, string companyName, int expectedCount, CancellationToken token)
        {
            using (var session = store.OpenAsyncSession())
            {
                var itemsCount = await session
                    .Query<DocumentsIndex.Result, DocumentsIndex>()
                    .Where(x => x.CompanyName == companyName)
                    .CountAsync(token);

                Assert.Equal(expectedCount, itemsCount);
            }
        }

        private static async Task<int> AssertBatchCountProgress(IDocumentStore store, string indexName, Func<IAsyncDocumentSession, string, CancellationToken, Task<int>> getItemsCount, Func<CancellationToken, Task> modifyItem, CancellationToken token)
        {
            var batchCount = 0;
            var tcs = new TaskCompletionSource<object>();
            var locker = new SemaphoreSlim(1, 1);

            store.Changes().ForIndex(indexName).Subscribe(x =>
            {
                if (x.Type == IndexChangeTypes.BatchCompleted)
                {
                    using (Lock())
                    {
                        if (Interlocked.Increment(ref batchCount) > 1)
                            tcs.SetResult(null);
                    }
                }
            });

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.MaxNumberOfRequestsPerSession = 50;

                var itemsCount1 = await getItemsCount(session, _companyName1, token);
                Assert.Equal(_employeesCount, itemsCount1);

                var itemsCount2 = await getItemsCount(session, _companyName2, token);
                Assert.Equal(0, itemsCount2);

                await modifyItem(token);

                while (itemsCount1 > 0 || itemsCount2 != _employeesCount)
                {
                    token.ThrowIfCancellationRequested();

                    // wait for the batch to complete
                    Assert.True(await Task.WhenAny(tcs.Task, Task.Delay(10_000)) == tcs.Task);

                    using (Lock())
                    {
                        tcs = new TaskCompletionSource<object>();

                        var newItemsCount1 = await getItemsCount(session, _companyName1, token);
                        Assert.True(newItemsCount1 == 0 || (newItemsCount1 > 0 && itemsCount1 != newItemsCount1),
                            $"new count1: {newItemsCount1}, old count1: {itemsCount1}");
                        var newItemsCount2 = await getItemsCount(session, _companyName2, token);
                        Assert.True(newItemsCount2 == _employeesCount || (newItemsCount2 > 0 && itemsCount2 != newItemsCount2),
                            $"new count2: {newItemsCount2}, old count2: {itemsCount2}, employees count: {_employeesCount}");

                        itemsCount1 = newItemsCount1;
                        itemsCount2 = newItemsCount2;
                    }
                }
            }

            IDisposable Lock()
            {
                locker.Wait();

                return new DisposableAction(() => locker.Release());
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
