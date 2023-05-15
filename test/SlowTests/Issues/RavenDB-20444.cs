using FastTests;
using Raven.Client.Documents.Queries.Explanation;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20444 : RavenTestBase
{
    public RavenDB_20444(ITestOutputHelper output) : base(output)
    {
    }
    
    private record Doc(string StrVal, int? NumVal);
    
    [RavenFact(RavenTestCategory.Querying)]
    public void BoostingIsNotAppliedToCorrectSubclause()
    {
        using var store = GetDocumentStore();
        using var session = store.OpenSession();
        var query = session.Advanced.DocumentQuery<Doc>()
            .OpenSubclause() // boost(
                .Search(x => x.StrVal, "match") // search(StrVa;, $p0)
                .AndAlso() // and
                .OpenSubclause() // (
                    .WhereGreaterThanOrEqual(x => x.NumVal, 0) //NumVal >= $p1
                    .OrElse() // or
                    .WhereEquals(x => x.NumVal, (int?)null) // NumVal = $p2
                .CloseSubclause() // )
            .CloseSubclause() // 
            .Boost(0) // , $p3)
            .OrderByScore()
            .OrderByDescending(x => x.NumVal)
            .IncludeExplanations(out Explanations explanations);

        var rql = query.ToString();
        Assert.Equal("from 'Docs' where boost(search(StrVal, $p0) and (NumVal >= $p1 or NumVal = $p2), $p3) order by score(), NumVal as long desc include explanations()", rql);
    }

}
