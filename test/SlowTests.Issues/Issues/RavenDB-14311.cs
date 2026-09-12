using FastTests;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_14311 : RavenTestBase
{
    public RavenDB_14311(ITestOutputHelper output) : base(output)
    {
    }

    private sealed class Doc
    {
        public string Id { get; set; }
        public string StrVal { get; set; }
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Rql)]
    public void InvalidQueryWithExtraParenthesesShouldThrowSyntaxErrorNotNre()
    {
        using var store = GetDocumentStore();
        using var session = store.OpenSession();

        // Note the extra '()' after search(...) - this is invalid RQL syntax.
        // Previously this crashed the parser with a bare NullReferenceException (RavenDB-14311);
        // it should now report a readable syntax error instead.
        var query = session.Advanced.RawQuery<Doc>("from docs where search(StrVal, \"a\")()");

        var ex = Assert.ThrowsAny<RavenException>(() => query.Count());

        Assert.Contains("Expected a method name before '('", ex.Message);
        Assert.DoesNotContain(nameof(System.NullReferenceException), ex.Message);
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Rql)]
    public void ValidSearchQueryShouldStillWork()
    {
        using var store = GetDocumentStore();
        using var session = store.OpenSession();

        // The fix must not reject a legitimate search() call.
        var count = session.Advanced.RawQuery<Doc>("from docs where search(StrVal, \"a\")").Count();

        Assert.Equal(0, count);
    }
}
