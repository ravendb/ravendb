using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15754 : RavenTestBase
    {
        public RavenDB_15754(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanIndexReferencedDocument()
        {
            using (var store = GetDocumentStore())
            {
                const string companyName1 = "Hibernating Rhinos";
                const string companyName2 = "HR";
                var company = new Company
                {
                    Name = companyName1
                };

                var employeesCount = 1_000_000;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();

                    using (var bulk = store.BulkInsert())
                    {
                        for (var i = 0; i < employeesCount; i++)
                        {
                            await bulk.StoreAsync(new Employee
                            {
                                CompanyId = company.Id
                            });
                        }
                    }
                }

                new Index().Execute(store);

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(3));
                await AssertCount(companyName1);

                using (var session = store.OpenAsyncSession())
                {
                    company.Name = companyName2;
                    await session.StoreAsync(company, company.Id);
                    await session.SaveChangesAsync();
                }

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(5));
                await AssertCount(companyName2);

                async Task AssertCount(string companyName)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var itemsCount = await session.Query<Index.Result, Index>()
                            .Where(x => x.CompanyName == companyName).CountAsync();

                        Assert.Equal(employeesCount, itemsCount);
                    }
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
