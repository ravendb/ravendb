using System;
using System.Linq;
using FastTests;
using Orders;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25281_NestedWhen : RavenTestBase
{
    public RavenDB_25281_NestedWhen(ITestOutputHelper output) : base(output)
    {
    }

    // A when() guard nested inside an AND group must be honored. When the guard is false the
    // guarded sub-clause collapses to its enclosing operator's identity (MatchAll under AND),
    // so the group degrades to the remaining AND members.
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void When_NestedInAndGroup_IsHonored(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Employee { FirstName = "Alice", HiredAt = DateTime.UtcNow.AddDays(-1) });
            session.Store(new Employee { FirstName = "Bob", HiredAt = DateTime.UtcNow.AddDays(-1) });
            session.Store(new Employee { FirstName = "Bob", HiredAt = DateTime.UtcNow.AddYears(1) });
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            // Guard TRUE: group == (FirstName='Bob' AND HiredAt<=now()) -> Alice + past Bob = 2
            var guardOn = session.Advanced
                .RawQuery<Employee>("from Employees where FirstName = 'Alice' or (FirstName = 'Bob' and when($p0 > 10, HiredAt <= now()))")
                .AddParameter("p0", 20)
                .WaitForNonStaleResults()
                .ToList();
            Assert.Equal(2, guardOn.Count);

            // Guard FALSE: when() collapses to MatchAll under AND, group == FirstName='Bob' -> Alice + both Bobs = 3
            var guardOff = session.Advanced
                .RawQuery<Employee>("from Employees where FirstName = 'Alice' or (FirstName = 'Bob' and when($p0 > 10, HiredAt <= now()))")
                .AddParameter("p0", 5)
                .WaitForNonStaleResults()
                .ToList();
            Assert.Equal(3, guardOff.Count);
        }
    }

    // A when() guard nested inside an OR group must be honored. When the guard is false the
    // guarded sub-clause collapses to its enclosing operator's identity (MatchNothing under OR),
    // so the group degrades to the remaining OR members.
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.All)]
    public void When_NestedInOrGroup_IsHonored(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Employee { FirstName = "Bob", HiredAt = DateTime.UtcNow.AddYears(1) });
            session.Store(new Employee { FirstName = "Bob", HiredAt = DateTime.UtcNow.AddDays(-1) });
            session.Store(new Employee { FirstName = "Alice", HiredAt = DateTime.UtcNow.AddDays(-1) });
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            // Guard TRUE: group == (HiredAt>now() OR FirstName='Bob') -> for Bob always true -> both Bobs = 2
            var guardOn = session.Advanced
                .RawQuery<Employee>("from Employees where FirstName = 'Bob' and (HiredAt > now() or when($p0 > 10, FirstName = 'Bob'))")
                .AddParameter("p0", 20)
                .WaitForNonStaleResults()
                .ToList();
            Assert.Equal(2, guardOn.Count);

            // Guard FALSE: when() collapses to MatchNothing under OR, group == HiredAt>now() -> only future Bob = 1
            var guardOff = session.Advanced
                .RawQuery<Employee>("from Employees where FirstName = 'Bob' and (HiredAt > now() or when($p0 > 10, FirstName = 'Bob'))")
                .AddParameter("p0", 5)
                .WaitForNonStaleResults()
                .ToList();
            Assert.Equal(1, guardOff.Count);
        }
    }
}
