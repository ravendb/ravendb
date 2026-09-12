using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Integrations.PostgreSQL.PowerBI;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Integrations.PostgreSQL;

public class RavenDB_26997 : RavenTestBase
{
    public RavenDB_26997(ITestOutputHelper output) : base(output)
    {
    }

    private class Company
    {
        public string Name { get; set; }
        public string Phone { get; set; }
    }

    [RavenFact(RavenTestCategory.PostgreSql | RavenTestCategory.PowerBi)]
    public async Task ExplicitProjection_PreviewAndLoad_ReturnSameColumns()
    {
        using var store = GetDocumentStore();
        using (var session = store.OpenSession())
        {
            session.Store(new Company { Name = "Acme", Phone = "123" });
            session.Store(new Company { Name = "Beta", Phone = "456" });
            session.SaveChanges();
        }

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        const string rawLoadSql = """
            SELECT "Name", "Phone"
            FROM "public"."Companies"
            LIMIT 10
            """;

        const string wrappedPreviewSql = """
            select * from
            (
                SELECT "Name", "Phone"
                FROM "public"."Companies"
                LIMIT 10
            ) "_"
            """;

        Assert.True(PowerBIFetchQuery.TryParse(rawLoadSql, Array.Empty<int>(), database, out var loadQuery));
        Assert.True(PowerBIFetchQuery.TryParse(wrappedPreviewSql, Array.Empty<int>(), database, out var previewQuery));

        using (loadQuery)
        using (previewQuery)
        {
            var loadColumns = (await loadQuery.Init()).Select(c => c.Name).ToArray();
            var previewColumns = (await previewQuery.Init()).Select(c => c.Name).ToArray();

            Assert.Equal(new[] { "Name", "Phone" }, previewColumns);
            Assert.Equal(previewColumns, loadColumns);
        }
    }
}
