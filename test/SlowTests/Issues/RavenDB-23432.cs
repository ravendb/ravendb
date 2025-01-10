using FastTests;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23432(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Querying)]
    public void MissingOperatorAfterLastTokenWillThrowCorrectException()
    {
        using var store = GetDocumentStore();
        using var session = store.OpenSession();
        session.Store(new Product(){Name = "Test",Discontinued = true});
        session.SaveChanges();

        var invalidFilterClauseRql = @"from ""Products"" where Discontinued = true filter Name";

        var exception = Assert.Throws<InvalidQueryException>(() => session.Advanced.RawQuery<Product>(invalidFilterClauseRql).WaitForNonStaleResults().ToList());
        Assert.Contains("Expected operator after 'Name' field, but found nothing.", exception.Message);
    }
}
