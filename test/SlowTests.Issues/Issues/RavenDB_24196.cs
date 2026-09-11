using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_24196 : RavenTestBase
{
    public RavenDB_24196(ITestOutputHelper output) : base(output)
    {
    }

    // A search with the default Guess options groups itself with the search before it, and that group is joined to
    // whatever precedes it by its first member's flag. The flag used to be written inside the group's parentheses -
    // "or (and search(...) or search(...))" - which the server rejected.
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void AGroupOfSearchesJoinsThePreviousSearchWithItsFirstMembersFlag(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Dto { Name = "name with spaces" });
            session.Store(new Dto { Name = "spaces only" });
            session.Store(new Dto { Name = "name only" });
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            var query = session.Query<Dto>()
                .Customize(x => x.WaitForNonStaleResults())
                .Search(x => x.Name, "spaces")
                .Search(x => x.Name, "name", options: SearchOptions.And)
                .Search(x => x.Name, "name spaces", @operator: SearchOperator.Or);

            Assert.Equal("from 'Dtos' where search(Name, $p0) and (search(Name, $p1) or search(Name, $p2))", query.ToString());
            Assert.Equal(new[] { "name with spaces", "spaces only" }, query.ToList().Select(x => x.Name).OrderBy(x => x));

            // without the grouping the flags read left to right and RQL precedence binds the and first
            var ungrouped = session.Query<Dto>()
                .Customize(x => x.WaitForNonStaleResults())
                .Search(x => x.Name, "spaces")
                .Search(x => x.Name, "name", options: SearchOptions.And)
                .Search(x => x.Name, "name spaces", options: SearchOptions.Or, @operator: SearchOperator.Or);

            Assert.Equal("from 'Dtos' where search(Name, $p0) and search(Name, $p1) or search(Name, $p2)", ungrouped.ToString());
            Assert.Equal(3, ungrouped.ToList().Count);
        }
    }

    private class Dto
    {
        public string Id { get; set; }

        public string Name { get; set; }
    }
}
