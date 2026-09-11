using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_22364 : RavenTestBase
{
    public RavenDB_22364(ITestOutputHelper output) : base(output)
    {
    }

    // A clause after Search used to be joined with OR in DocumentQuery no matter what the default operator was,
    // while the same chain in LINQ joins with AND - so the two APIs returned different documents for the same intent.
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void AClauseAfterSearchJoinsWithTheDefaultOperatorInBothApis(Options options)
    {
        using var store = GetDocumentStore(options);
        new Employees_ByNameAndCategory().Execute(store);

        using (var session = store.OpenSession())
        {
            session.Store(new Employee { Id = "1", Name = "MA 1", Category = "A" });
            session.Store(new Employee { Id = "2", Name = "MA 2", Category = "A" });
            session.Store(new Employee { Id = "3", Name = "MA 3", Category = "C" });
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var documentQuery = session.Advanced.DocumentQuery<Employees_ByNameAndCategory.Result, Employees_ByNameAndCategory>()
                .Search(r => r.Name, "MA*")
                .WhereIn(r => r.Category, new[] { "A", "B" });

            var linq = session.Query<Employees_ByNameAndCategory.Result, Employees_ByNameAndCategory>()
                .Search(r => r.Name, "MA*")
                .Where(r => r.Category.In("A", "B"));

            Assert.Equal("from index 'Employees/ByNameAndCategory' where search(Name, $p0) and Category in ($p1)", documentQuery.ToString());
            Assert.Equal(2, documentQuery.ToList().Count);
            Assert.Equal(2, linq.ToList().Count);

            var orQuery = session.Advanced.DocumentQuery<Employees_ByNameAndCategory.Result, Employees_ByNameAndCategory>()
                .UsingDefaultOperator(QueryOperator.Or)
                .Search(r => r.Name, "MA*")
                .WhereIn(r => r.Category, new[] { "A", "B" });

            Assert.Equal("from index 'Employees/ByNameAndCategory' where search(Name, $p0) or Category in ($p1)", orQuery.ToString());
            Assert.Equal(3, orQuery.ToList().Count);

            // nothing to join after Intersect(), so an And flag must not write an operator there
            var intersect = session.Query<Employees_ByNameAndCategory.Result, Employees_ByNameAndCategory>()
                .Where(r => r.Category == "A")
                .Intersect()
                .Search(r => r.Name, "MA*", options: SearchOptions.And);

            Assert.Equal("from index 'Employees/ByNameAndCategory' where intersect((Category = $p0), search(Name, $p1))", intersect.ToString().TrimEnd());
            if (options.SearchEngineMode == RavenSearchEngineMode.Lucene) // Corax does not support intersect
                Assert.Equal(2, intersect.ToList().Count);
        }
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void DocumentQueryJoinsSearchLikeAnyOtherClause()
    {
        using var store = GetDocumentStore();
        using var session = store.OpenSession();

        // the default operator applies after a search
        Assert.Equal("from 'Employees' where search(Name, $p0) and Category = $p1",
            session.Advanced.DocumentQuery<Employee>().Search(x => x.Name, "ma").WhereEquals(x => x.Category, "A").ToString());

        Assert.Equal("from 'Employees' where search(Name, $p0) or Category = $p1",
            session.Advanced.DocumentQuery<Employee>().UsingDefaultOperator(QueryOperator.Or).Search(x => x.Name, "ma").WhereEquals(x => x.Category, "A").ToString());

        // ...also when the search sits inside a closed subclause
        Assert.Equal("from 'Employees' where (search(Name, $p0)) and Category = $p1",
            session.Advanced.DocumentQuery<Employee>().OpenSubclause().Search(x => x.Name, "ma").CloseSubclause().WhereEquals(x => x.Category, "A").ToString());

        // consecutive searches are alternatives
        Assert.Equal("from 'Employees' where search(Name, $p0) or search(Category, $p1)",
            session.Advanced.DocumentQuery<Employee>().Search(x => x.Name, "ma").Search(x => x.Category, "a").ToString());

        // ...and DocumentQuery never inserts parentheses, so next to another clause they follow RQL precedence
        // unless the caller groups them - the LINQ provider does that on its own
        Assert.Equal("from 'Employees' where search(Name, $p0) or search(Surname, $p1) and Category = $p2",
            session.Advanced.DocumentQuery<Employee>().Search(x => x.Name, "ma").Search(x => x.Surname, "ko").WhereEquals(x => x.Category, "A").ToString());

        Assert.Equal("from 'Employees' where (search(Name, $p0) or search(Surname, $p1)) and Category = $p2",
            session.Advanced.DocumentQuery<Employee>().OpenSubclause().Search(x => x.Name, "ma").Search(x => x.Surname, "ko").CloseSubclause().WhereEquals(x => x.Category, "A").ToString());

        // an explicit joiner always wins
        Assert.Equal("from 'Employees' where search(Name, $p0) or Category = $p1",
            session.Advanced.DocumentQuery<Employee>().Search(x => x.Name, "ma").OrElse().WhereEquals(x => x.Category, "A").ToString());

        Assert.Equal("from 'Employees' where search(Name, $p0) and search(Category, $p1)",
            session.Advanced.DocumentQuery<Employee>().Search(x => x.Name, "ma").AndAlso().Search(x => x.Category, "a").ToString());
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void LinqSearchOptionsJoinToThePreviousClauseAndNotToTheNextOne()
    {
        using var store = GetDocumentStore();
        using var session = store.OpenSession();

        // Or and And relate a search to the search before it - And overrides the default OR between searches - and
        // after a non-search clause neither of them matters: the default operator applies
        Assert.Equal("from 'Employees' where search(Name, $p0) or search(Category, $p1)",
            session.Query<Employee>().Search(x => x.Name, "ma").Search(x => x.Category, "a", options: SearchOptions.Or).ToString());

        Assert.Equal("from 'Employees' where search(Name, $p0) and search(Category, $p1)",
            session.Query<Employee>().Search(x => x.Name, "ma", options: SearchOptions.Or).Search(x => x.Category, "a", options: SearchOptions.And).ToString());

        Assert.Equal("from 'Employees' where (Category = $p0) and search(Name, $p1)",
            session.Query<Employee>().Where(x => x.Category == "A").Search(x => x.Name, "ma", options: SearchOptions.Or).ToString());

        Assert.Equal("from 'Employees' where (Category = $p0) and search(Name, $p1)",
            session.Query<Employee>().Where(x => x.Category == "A").Search(x => x.Name, "ma", options: SearchOptions.And).ToString());

        // ...and say nothing about the clause that follows, which joins with AND like after any other search
        Assert.Equal("from 'Employees' where search(Name, $p0) and (Category = $p1)",
            session.Query<Employee>().Search(x => x.Name, "ma").Where(x => x.Category == "A").ToString());

        Assert.Equal("from 'Employees' where search(Name, $p0) and (Category = $p1)",
            session.Query<Employee>().Search(x => x.Name, "ma", options: SearchOptions.Or).Where(x => x.Category == "A").ToString());

        Assert.Equal("from 'Employees' where (exists(Name) and not search(Name, $p0)) and (Category = $p1)",
            session.Query<Employee>().Search(x => x.Name, "ma", options: SearchOptions.Not).Where(x => x.Category == "A").ToString());

        // consecutive search statements stay alternatives, negated ones included
        Assert.Equal("from 'Employees' where (search(Name, $p0) or search(Category, $p1))",
            session.Query<Employee>().Search(x => x.Name, "ma").Search(x => x.Category, "a").ToString());

        Assert.Equal("from 'Employees' where search(Name, $p0) or (exists(Category) and not search(Category, $p1))",
            session.Query<Employee>().Search(x => x.Name, "ma").Search(x => x.Category, "a", options: SearchOptions.Not).ToString());

        // a group of searches joins the clause before it with the first search's flag
        Assert.Equal("from 'Employees' where (Category = $p0) and (search(Name, $p1) or search(Surname, $p2))",
            session.Query<Employee>().Where(x => x.Category == "A").Search(x => x.Name, "ma", options: SearchOptions.And).Search(x => x.Surname, "ko").ToString());

        // converting to a DocumentQuery and back does not change the operator
        var roundTrip = session.Advanced.DocumentQuery<Employee>().ToQueryable().Search(x => x.Name, "ma").ToDocumentQuery().ToQueryable().Where(x => x.Category == "A");
        Assert.Equal("from 'Employees' where search(Name, $p0) and Category = $p1", roundTrip.ToString());
    }

    private class Employee
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public string Surname { get; set; }

        public string Category { get; set; }
    }

    private class Employees_ByNameAndCategory : AbstractIndexCreationTask<Employee, Employees_ByNameAndCategory.Result>
    {
        public class Result
        {
            public string Name { get; set; }

            public string Category { get; set; }
        }

        public Employees_ByNameAndCategory()
        {
            Map = employees => from e in employees
                               select new Result { Name = e.Name, Category = e.Category };

            StoreAllFields(FieldStorage.Yes);
        }
    }
}
