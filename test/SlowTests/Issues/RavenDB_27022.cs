using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_27022 : RavenTestBase
{
    public RavenDB_27022(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void ReservedFieldNameCannotBeUsedInADynamicQuery(Options options)
    {
        using var store = GetDocumentStore(options);

        StoreOrder(store);

        AssertRejected(store,
            "from 'Orders' where search(__all_fields, '*')",
            "from 'Orders' where __all_fields = 'companies/1'",
            "from 'Orders' where exists(__all_fields)",
            "from 'Orders' order by __all_fields",
            "from 'Orders' group by __all_fields select count()",
            "from 'Orders' select suggest(__all_fields, 'companies')");
    }

    // highlighting is not supported in a sharded database
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ReservedFieldNameCannotBeHighlighted(Options options)
    {
        using var store = GetDocumentStore(options);

        StoreOrder(store);

        AssertRejected(store, "from 'Orders' where search(Company, 'companies') include highlight(__all_fields, 18, 2)");
    }

    private static void StoreOrder(IDocumentStore store)
    {
        using var session = store.OpenSession();

        session.Store(new Order { Company = "companies/1" });
        session.SaveChanges();
    }

    private static void AssertRejected(IDocumentStore store, params string[] queries)
    {
        foreach (var query in queries)
        {
            using var session = store.OpenSession();

            var e = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<Order>(query).ToList());
            Assert.Contains("__all_fields", e.Message);
        }

        // the query must be rejected before an auto index is built from it, otherwise the index is left in an error state
        Assert.Empty(store.Maintenance.Send(new GetIndexNamesOperation(0, 25)));
    }

    private sealed class Order
    {
        public string Company { get; set; }
    }
}
