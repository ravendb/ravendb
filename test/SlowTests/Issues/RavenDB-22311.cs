using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22311 : RavenTestBase
{
    public RavenDB_22311(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Lucene | RavenTestCategory.Querying)]
    public void AlphanumericalWillShiftWithGoodOffsetSize()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Lucene));
        using var session = store.OpenSession();
        session.Store(new Order(){Company = "🚀1t"});
        session.Store(new Order(){Company = "🚀1"});
        session.SaveChanges();

        var result = session.Query<Order>()
            .Customize(c => c.WaitForNonStaleResults())
            .OrderBy(x => x.Company, OrderingType.AlphaNumeric)
            .ToList();
        
        Assert.Equal("🚀1", result[0].Company);
        Assert.Equal("🚀1t", result[1].Company);
    }
}
