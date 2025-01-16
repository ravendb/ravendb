using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
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
                company.Name = CompanyName2;
                session.Advanced.WaitForIndexesAfterSaveChanges();
                await session.StoreAsync(company, company.Id);
                await session.SaveChangesAsync();
            }
            await Indexes.WaitForIndexingAsync(store, timeout: TimeSpan.FromMinutes(3));
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
                Assert.Equal(10, result[0].Count);
            }
            using (var session = store.OpenAsyncSession())
            {
                company.Name = CompanyName2;
                session.Advanced.WaitForIndexesAfterSaveChanges();
                await session.StoreAsync(company, company.Id);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var result = await session.Query<MapReduceIndex.Result, MapReduceIndex>().ToListAsync();

                Assert.Equal(1, result.Count);
                Assert.Equal(CompanyName2, result[0].CompanyName);
                Assert.Equal(10, result[0].Count);
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
}
