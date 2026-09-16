using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Corax;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27348(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void MethodsCoraxDoesNotSupportMustThrowNotSupportedInCoraxException(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            session.Store(new Employee { Notes = "fluent french speaker", Age = 30 });
            session.SaveChanges();
        }

        new Employees_ByNotesAndAge().Execute(store);
        Indexes.WaitForIndexing(store);

        var cases = new[]
        {
            ("proximity(search(Notes, 'fluent french'), 0)", "proximity over search() method"),
            ("fuzzy(Notes = 'french', 0.5)", "fuzzy() method"),
            ("lucene(Notes, 'french')", "lucene() method"),
            ("intersect(Notes = 'french', Age = 30)", "intersect queries"),
        };

        foreach (var (where, expected) in cases)
        {
            using var session = store.OpenSession();
            var query = () => session.Advanced.RawQuery<Employee>($"from index 'Employees/ByNotesAndAge' where {where}").ToList();

            if (options.SearchEngineMode == RavenSearchEngineMode.Corax)
            {
                var e = Assert.Throws<NotSupportedInCoraxException>(query);
                Assert.Contains(expected, e.Message);
            }
            else
            {
                Assert.Equal(1, query().Count);
            }
        }
    }

    private class Employee
    {
        public string Notes { get; set; }

        public int Age { get; set; }
    }

    private class Employees_ByNotesAndAge : AbstractIndexCreationTask<Employee>
    {
        public Employees_ByNotesAndAge()
        {
            Map = employees => from employee in employees
                               select new { employee.Notes, employee.Age };

            Index(x => x.Notes, FieldIndexing.Search);
        }
    }
}
