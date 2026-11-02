using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_27517 : RavenTestBase
{
    public RavenDB_27517(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
    public void APredicateOnATerminalOperatorMustNotBindToTheLastClauseOnly()
    {
        using var store = GetDocumentStore();

        using (var session = store.OpenSession())
        {
            session.Store(new Employee { Name = "n1", Category = "A" });
            session.Store(new Employee { Name = "n2", Category = "A" });
            session.Store(new Employee { Name = "n3", Category = "B" });
            session.Store(new Employee { Name = "zzz", Category = "B" });
            session.SaveChanges();
        }

        using var s = store.OpenSession();

        const string expected = "from 'Employees' where Name != $p0 and (Category = $p1 or Category = $p2)";

        var cases = new (string Name, Func<IRavenQueryable<Employee>, object> Run)[]
        {
            ("Count", q => q.Count(x => x.Category == "A" || x.Category == "B")),
            ("LongCount", q => q.LongCount(x => x.Category == "A" || x.Category == "B")),
            ("Any", q => q.Any(x => x.Category == "A" || x.Category == "B")),
            ("First", q => q.First(x => x.Category == "A" || x.Category == "B")),
            ("FirstOrDefault", q => q.FirstOrDefault(x => x.Category == "A" || x.Category == "B")),
            ("Single", q => q.Single(x => x.Category == "A" || x.Category == "B")),
            ("SingleOrDefault", q => q.SingleOrDefault(x => x.Category == "A" || x.Category == "B"))
        };

        foreach (var (name, run) in cases)
        {
            string rql = null;

            try
            {
                run(Chained(s, i => rql = i.Query));
            }
            catch (InvalidOperationException)
            {
                // Single over more than one match; the query text is captured before execution
            }

            Assert.True(rql != null && rql.StartsWith(expected, StringComparison.Ordinal),
                $"{name}{Environment.NewLine}expected: {expected}{Environment.NewLine}actual:   {rql}");
        }

        // the observable symptom: the predicate widened the result instead of narrowing it
        Assert.Equal(3, Chained(s).Count(x => x.Category == "A" || x.Category == "B"));
        Assert.Equal(3L, Chained(s).LongCount(x => x.Category == "A" || x.Category == "B"));

        // a predicate that is not a composite must stay unparenthesized
        string simple = null;
        Chained(s, i => simple = i.Query).Count(x => x.Category == "A");
        Assert.StartsWith("from 'Employees' where Name != $p0 and Category = $p1", simple, StringComparison.Ordinal);
    }

    private static IRavenQueryable<Employee> Chained(IDocumentSession session, Action<IndexQuery> onQuery = null)
    {
        return session.Query<Employee>()
            .Customize(x =>
            {
                x.WaitForNonStaleResults();

                if (onQuery != null)
                    x.BeforeQueryExecuted(onQuery);
            })
            .Where(x => x.Name != "zzz");
    }

    private sealed class Employee
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public string Category { get; set; }
    }
}
