using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_14806 : RavenTestBase
{
    public RavenDB_14806(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public async Task CanQueryOverReservedPropertieS(Options options)
    {
        string companyId = "companies/1-A";

        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenAsyncSession();
            await session.StoreAsync(new ContractClause() {CompanyId = companyId});
            await session.SaveChangesAsync();
        }
        {
            using var session = store.OpenAsyncSession();
            var query = session.Query<ContractClause>()
                .Where(c => c.CompanyId == companyId)
                .OrderBy(c => c.Group)
                .ThenBy(c => c.SortOrder)
                .Select(c => new ContractClauseListItem
                {
                    Id = c.Id,
                    Name = c.Name,
                    Group = c.Group,
                    SortOrder = c.SortOrder,
                    Description = c.Description
                });
WaitForUserToContinueTheTest(store);
            var result = await query.ToListAsync();
            Assert.Equal(1, result.Count);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public async Task CanQueryWithGroupOverStaticIndex(Options options)
    {
        using var store = GetDocumentStore(options);
        {
            using var session = store.OpenSession();
            session.Store(new Data() {Name = "Test"});
            session.SaveChanges();
        }
        var index = new DataIndex();
        await index.ExecuteAsync(store);
        Indexes.WaitForIndexing(store);

        {
            using var session = store.OpenAsyncSession();

            var query = (IQueryable<Data>)session.Query<Data, DataIndex>();
            query = query.ToAsyncDocumentQuery().WhereEquals(m => m.Name, "test").ToQueryable();
            query = query.OrderBy(m => m.Name);
            var q = (from cor in query
                select new Dto {Id = cor.Id, Name = cor.Name, Group = cor.Group}).Take(100);
            var results = (await q.ToListAsync()).OfType<ITestDto>();
            foreach (var result in results)
            {
                Assert.Equal(result.Name, "Test");
            }
        }
    }

    interface ITestDto
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }

        public string Group { get; set; }
    }

    private class Data
    {
        public string Id { get; set; }
        public string Name { get; set; }

        public string Group { get; set; }
    }

    private class DataIndex : AbstractIndexCreationTask<Data>
    {
        public DataIndex()
        {
            Map = datas => datas.Select(i => new {Name = i.Name});
        }
    }

    private class ContractClauseListItem
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public string Group { get; set; }
        public int SortOrder { get; set; }
    }

    private class ContractClause
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public string CompanyId { get; set; }
        public string Group { get; set; }
        public int SortOrder { get; set; }
    }
}
