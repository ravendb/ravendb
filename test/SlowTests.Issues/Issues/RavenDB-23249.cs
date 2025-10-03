using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Indexes.TimeSeries;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23249 : RavenTestBase
{
    private const int EmployeesCount = 10;
    private const string CommonName = "Companies";
    private const string CompanyName1 = "Hibernating Rhinos";
    private const string CompanyName2 = "RavenDB";

    public RavenDB_23249(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task CanIndexAllDocumentsReferencesMapIndex()
    {
        using (var store = GetDocumentStore())
        {
            var company = new Company { Name = CompanyName1 };

            await new MapIndex().ExecuteAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);

                for (var i = 0; i < EmployeesCount; i++)
                {
                    await session.StoreAsync(new Employee
                    {
                        CompanyId = company.Id
                    });
                }

                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromMinutes(3));

            using (var session = store.OpenAsyncSession())
            {
                var count = await session.Query<MapIndex.Result, MapIndex>()
                    .Where(x => x.CompanyName == CompanyName1).CountAsync();

                Assert.Equal(EmployeesCount, count);
            }

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges();

                company.Name = CompanyName2;
                await session.StoreAsync(company, company.Id);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var count = await session.Query<MapIndex.Result, MapIndex>()
                    .Where(x => x.CompanyName == CompanyName1).CountAsync();

                Assert.Equal(0, count);

                count = await session.Query<MapIndex.Result, MapIndex>()
                    .Where(x => x.CompanyName == CompanyName2).CountAsync();

                Assert.Equal(EmployeesCount, count);
            }
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task CanIndexAllDocumentsReferencesMapReduceIndex()
    {
        using (var store = GetDocumentStore())
        {
            var company = new Company { Name = CompanyName1 };

            await new MapReduceIndex().ExecuteAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);

                for (var i = 0; i < EmployeesCount; i++)
                {
                    await session.StoreAsync(new Employee
                    {
                        CompanyId = company.Id
                    });
                }

                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromMinutes(3));

            using (var session = store.OpenAsyncSession())
            {
                var result = await session.Query<MapReduceIndex.Result, MapReduceIndex>().ToListAsync();

                Assert.Equal(1, result.Count);
                Assert.Equal(CompanyName1, result[0].CompanyName);
                Assert.Equal(EmployeesCount, result[0].Count);
            }
            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges();
                
                company.Name = CompanyName2;
                await session.StoreAsync(company, company.Id);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var result = await session.Query<MapReduceIndex.Result, MapReduceIndex>().ToListAsync();

                Assert.Equal(1, result.Count);
                Assert.Equal(CompanyName2, result[0].CompanyName);
                Assert.Equal(EmployeesCount, result[0].Count);
            }
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task CanIndexAllDocumentsReferencesMapCountersIndex()
    {
        using (var store = GetDocumentStore())
        {
            var company = new Company
            {
                Id = CommonName,
                Name = CompanyName1
            };

            await new MapCountersIndex().ExecuteAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                await session.SaveChangesAsync();

                for (var i = 0; i < EmployeesCount; i++)
                {
                    var employee = new Employee();
                    await session.StoreAsync(employee);
                    session.CountersFor(employee.Id).Increment(CommonName);
                }

                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromMinutes(3));

            using (var session = store.OpenAsyncSession())
            {
                var count = await session.Query<MapCountersIndex.Result, MapCountersIndex>()
                    .Where(x => x.CompanyName == CompanyName1).CountAsync();

                Assert.Equal(EmployeesCount, count);
            }

            WaitForUserToContinueTheTest(store);

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges();

                company.Name = CompanyName2;
                await session.StoreAsync(company, company.Id);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var count = await session.Query<MapCountersIndex.Result, MapCountersIndex>()
                    .Where(x => x.CompanyName == CompanyName1).CountAsync();

                Assert.Equal(0, count);

                count = await session.Query<MapCountersIndex.Result, MapCountersIndex>()
                    .Where(x => x.CompanyName == CompanyName2).CountAsync();

                Assert.Equal(EmployeesCount, count);
            }
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task CanIndexAllDocumentsReferencesMapReduceCountersIndex()
    {
        using (var store = GetDocumentStore())
        {
            var company = new Company
            {
                Id = CommonName,
                Name = CompanyName1
            };

            await new MapReduceCountersIndex().ExecuteAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                await session.SaveChangesAsync();

                for (var i = 0; i < EmployeesCount; i++)
                {
                    var employee = new Employee();
                    await session.StoreAsync(employee);
                    session.CountersFor(employee.Id).Increment(CommonName);
                }

                await session.SaveChangesAsync();
            }

            await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromMinutes(3));

            using (var session = store.OpenAsyncSession())
            {
                var result = await session.Query<MapReduceCountersIndex.Result, MapReduceCountersIndex>().ToListAsync();

                Assert.Equal(1, result.Count);
                Assert.Equal(CompanyName1, result[0].CompanyName);
                Assert.Equal(EmployeesCount, result[0].Count);
            }

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges();

                company.Name = CompanyName2;
                await session.StoreAsync(company, company.Id);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var result = await session.Query<MapReduceCountersIndex.Result, MapReduceCountersIndex>().ToListAsync();

                Assert.Equal(1, result.Count);
                Assert.Equal(CompanyName2, result[0].CompanyName);
                Assert.Equal(EmployeesCount, result[0].Count);
            }
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task CanIndexAllDocumentsReferencesMapTimeSeriesIndex()
    {
        using (var store = GetDocumentStore())
        {
            var company = new Company
            {
                Id = CommonName,
                Name = CompanyName1
            };

            await new MapTimeSeriesIndex().ExecuteAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                await session.SaveChangesAsync();
            }

            using (var bulk = store.BulkInsert())
            {
                var baseDate = DateTime.UtcNow;

                for (var i = 0; i < EmployeesCount; i++)
                {
                    var employee = new Employee();
                    await bulk.StoreAsync(employee);

                    using (var ts = bulk.TimeSeriesFor(employee.Id, CommonName))
                    {
                        await ts.AppendAsync(baseDate, 1, company.Id);
                    }
                }
            }

            await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromMinutes(3));

            using (var session = store.OpenAsyncSession())
            {
                var count = await session.Query<MapTimeSeriesIndex.Result, MapTimeSeriesIndex>()
                    .Where(x => x.CompanyName == CompanyName1).CountAsync();

                Assert.Equal(EmployeesCount, count);
            }

            WaitForUserToContinueTheTest(store);

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges();

                company.Name = CompanyName2;
                await session.StoreAsync(company, company.Id);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var count = await session.Query<MapTimeSeriesIndex.Result, MapTimeSeriesIndex>()
                    .Where(x => x.CompanyName == CompanyName1).CountAsync();

                Assert.Equal(0, count);

                count = await session.Query<MapTimeSeriesIndex.Result, MapTimeSeriesIndex>()
                    .Where(x => x.CompanyName == CompanyName2).CountAsync();

                Assert.Equal(EmployeesCount, count);
            }
        }
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public async Task CanIndexAllDocumentsReferencesMapReduceTimeSeriesIndex()
    {
        using (var store = GetDocumentStore())
        {
            var company = new Company
            {
                Name = CompanyName1
            };

            await new MapReduceTimeSeriesIndex().ExecuteAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(company);
                await session.SaveChangesAsync();
            }

            using (var bulk = store.BulkInsert())
            {
                var baseDate = DateTime.UtcNow;

                for (var i = 0; i < EmployeesCount; i++)
                {
                    var employee = new Employee();
                    await bulk.StoreAsync(employee);

                    using (var ts = bulk.TimeSeriesFor(employee.Id, CommonName))
                    {
                        await ts.AppendAsync(baseDate, 1, company.Id);
                    }
                }
            }

            await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromMinutes(3));

            WaitForUserToContinueTheTest(store);

            using (var session = store.OpenAsyncSession())
            {
                var result = await session.Query<MapReduceTimeSeriesIndex.Result, MapReduceTimeSeriesIndex>().ToListAsync();

                Assert.Equal(1, result.Count);
                Assert.Equal(CompanyName1, result[0].CompanyName);
                Assert.Equal(EmployeesCount, result[0].Count);
            }

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.WaitForIndexesAfterSaveChanges();

                company.Name = CompanyName2;
                await session.StoreAsync(company, company.Id);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var result = await session.Query<MapReduceTimeSeriesIndex.Result, MapReduceTimeSeriesIndex>().ToListAsync();

                Assert.Equal(1, result.Count);
                Assert.Equal(CompanyName2, result[0].CompanyName);
                Assert.Equal(EmployeesCount, result[0].Count);
            }
        }
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

    private class MapIndex : AbstractIndexCreationTask<Employee>
    {
        public class Result
        {
            public string CompanyName { get; set; }
        }

        public MapIndex()
        {
            Map = employees =>
                from employee in employees
                select new Result
                {
                    CompanyName = LoadDocument<Company>(employee.CompanyId, Constants.Documents.Collections.AllDocumentsCollection).Name
                };
        }
    }

    private class MapReduceIndex : AbstractIndexCreationTask<Employee, MapReduceIndex.Result>
    {
        public class Result
        {
            public string CompanyName { get; set; }

            public int Count { get; set; }
        }

        public MapReduceIndex()
        {
            Map = employees =>
                from employee in employees
                select new Result
                {
                    CompanyName = LoadDocument<Company>(employee.CompanyId, Constants.Documents.Collections.AllDocumentsCollection).Name,
                    Count = 1
                };

            Reduce = results =>
                from result in results
                group result by result.CompanyName into g
                select new Result
                {
                    CompanyName = g.Key,
                    Count = g.Sum(x => x.Count)
                };
        }
    }

    private class MapCountersIndex : AbstractCountersIndexCreationTask<Employee>
    {
        public class Result
        {
            public string CompanyName { get; set; }
        }

        public MapCountersIndex()
        {
            AddMap(CommonName,
                counters => from counter in counters
                    select new Result
                    {
                        CompanyName = LoadDocument<Company>(counter.Name, Constants.Documents.Collections.AllDocumentsCollection).Name
                    });
        }
    }

    private class MapReduceCountersIndex : AbstractCountersIndexCreationTask<Employee, MapReduceCountersIndex.Result>
    {
        public class Result
        {
            public string CompanyName { get; set; }

            public int Count { get; set; }
        }

        public MapReduceCountersIndex()
        {
            AddMap(CommonName,
                counters => from counter in counters
                    select new Result
                    {
                        CompanyName = LoadDocument<Company>(counter.Name, Constants.Documents.Collections.AllDocumentsCollection).Name,
                        Count = 1
                    });

            Reduce = results => from r in results
                group r by r.CompanyName into g
                select new Result
                {
                    CompanyName = g.Select(x => x.CompanyName).FirstOrDefault(),
                    Count = g.Sum(x => x.Count)
                };
        }
    }

    private class MapTimeSeriesIndex : AbstractTimeSeriesIndexCreationTask<Employee>
    {
        public class Result
        {
            public string CompanyName { get; set; }
        }

        public MapTimeSeriesIndex()
        {
            AddMap(
                CommonName,
                timeSeries => from ts in timeSeries
                    from entry in ts.Entries
                    select new Result
                    {
                        CompanyName = LoadDocument<Company>(entry.Tag, Constants.Documents.Collections.AllDocumentsCollection).Name
                    });
        }
    }

    private class MapReduceTimeSeriesIndex : AbstractTimeSeriesIndexCreationTask<Employee, MapReduceTimeSeriesIndex.Result>
    {
        public class Result
        {
            public string CompanyName { get; set; }

            public int Count { get; set; }
        }

        public MapReduceTimeSeriesIndex()
        {
            AddMap(
                CommonName,
                timeSeries => from ts in timeSeries
                    from entry in ts.Entries
                    select new Result
                    {
                        CompanyName = LoadDocument<Company>(entry.Tag, Constants.Documents.Collections.AllDocumentsCollection).Name,
                        Count = 1
                    });

            Reduce = results => from r in results
                group r by r.CompanyName into g
                select new Result
                {
                    CompanyName = g.Key,
                    Count = g.Sum(x => x.Count)
                };
        }
    }
}
