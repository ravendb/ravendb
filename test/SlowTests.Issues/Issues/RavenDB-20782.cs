using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Queries.Highlighting;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20782 : RavenTestBase
{
    public RavenDB_20782(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public async Task CustomHighlightingViaDocumentQueryAsync(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenAsyncSession())
        {
            var employee = new Employee() {Notes = new List<string>() {"sales"}};
            await session.StoreAsync(employee);
            await session.SaveChangesAsync();

            HighlightingOptions tagsToUse = new HighlightingOptions {PreTags = new[] {"+++"}, PostTags = new[] {"+++"}};

            var employeesResults = await session.Advanced.AsyncDocumentQuery<Employee>()
                .WaitForNonStaleResults()
                .Search(x => x.Notes, "sales")
                .Highlight(x => x.Notes, 35, 4, tagsToUse, out Highlightings salesHighlights)
                .ToListAsync();

            Assert.Equal("+++sales+++", salesHighlights.GetFragments(employee.Id).First());
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void CustomHighlightingViaDocumentQuery(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            var employee = new Employee() {Notes = new List<string>() {"sales"}};
            session.Store(employee);
            session.SaveChanges();

            HighlightingOptions tagsToUse = new HighlightingOptions {PreTags = new[] {"+++"}, PostTags = new[] {"+++"}};

            var employeesResults = session.Advanced.DocumentQuery<Employee>()
                .WaitForNonStaleResults()
                .Search(x => x.Notes, "sales")
                .Highlight(x => x.Notes, 35, 4, tagsToUse, out Highlightings salesHighlights)
                .ToList();

            Assert.Equal("+++sales+++", salesHighlights.GetFragments(employee.Id).First());
        }
    }
}
