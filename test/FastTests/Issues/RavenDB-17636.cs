using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues;

public class FilterTests : RavenTestBase
{
    public FilterTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void CanUseFilterAsContextualKeywordForBackwardCompatability(Options options)
    {
        using var store = GetDocumentStore(options);
        var data = GetDatabaseItems();
        Insert(store, data, options.DatabaseMode);
        // raw
        using (var s = store.OpenSession())
        {
            var result = s.Advanced
                .RawQuery<Employee>("from Employees filter where filter.Name = 'Jane' filter filter.Name ='Jane' select filter")
                .SingleOrDefault();

            Assert.Equal("Jane", result.Name);

            var c = s.Advanced.RawQuery<Employee>(@"
declare function filter(a) {
    return {filtered: true};
}
from Employees as a
select filter(a)").Count();

            Assert.Equal(3, c);

        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(1, SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    [RavenData(3, SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Sharded)]
    public void CanUseFilterWithCollectionQuery(Options options, long scannedResults)
    {
        using var store = GetDocumentStore(options);
        var data = GetDatabaseItems();
        Insert(store, data, options.DatabaseMode);
        Employee result;
        QueryStatistics stats;
        // raw
        using (var s = store.OpenSession())
        {
            result = s.Advanced.RawQuery<Employee>("from Employees filter Name = 'Jane'").SingleOrDefault();
            Assert.Equal("Jane", result.Name);

            result = s.Advanced.DocumentQuery<Employee>().Filter(p => p.Equals(a => a.Name, "Jane")).SingleOrDefault();
            Assert.Equal("Jane", result.Name);

            result = s.Query<Employee>().Filter(f => f.Name == "Jane").SingleOrDefault();
            Assert.Equal("Jane", result.Name);
        }

        // scan results
        using (var s = store.OpenSession())
        {
            //WaitForUserToContinueTheTest(store);
            result = s.Advanced
                .RawQuery<Employee>("from Employees filter Active = false")
                .Statistics(out stats)
                .SingleOrDefault();
            Assert.Equal("Mark", result.Name);
            Assert.Equal(3, stats.ScannedResults);

            result = s.Advanced.DocumentQuery<Employee>()
                .Filter(f => f.Equals("Active", false))
                .Statistics(out stats)
                .SingleOrDefault();
            Assert.Equal("Mark", result.Name);
            Assert.Equal(3, stats.ScannedResults);

            result = s.Query<Employee>()
                .Filter(f => f.Active == false)
                .Statistics(out stats)
                .SingleOrDefault();
            Assert.Equal("Mark", result.Name);
            Assert.Equal(3, stats.ScannedResults);
        }

        // scan limit
        using (var s = store.OpenSession())
        {
            result = s.Advanced.RawQuery<Employee>("from Employees filter Active = true filter_limit 1")
                .Statistics(out stats)
                .SingleOrDefault();
            Assert.Equal("Jane", result.Name);
            Assert.Equal(scannedResults, stats.ScannedResults);

            result = s.Advanced.DocumentQuery<Employee>()
                .Filter(
                    f => f.Equals("Active", true), 1
                )
                .Statistics(out stats)
                .SingleOrDefault();
            Assert.Equal("Jane", result.Name);
            Assert.Equal(scannedResults, stats.ScannedResults);

            result = s.Query<Employee>()
                .Filter(f => f.Active == true, 1)
                .Statistics(out stats)
                .SingleOrDefault();
            Assert.Equal("Jane", result.Name);
            Assert.Equal(scannedResults, stats.ScannedResults);
        }

        // parameters
        using (var s = store.OpenSession())
        {
            var emp = s.Advanced.RawQuery<Employee>("from Employees filter Name = $name")
                .AddParameter("name", "Jane")
                .SingleOrDefault();
            Assert.Equal("Jane", emp.Name);
        }

        // alias
        using (var s = store.OpenSession())
        {
            var emp = s.Advanced
                .RawQuery<Employee>("from Employees as e filter e.Name = $name")
                .AddParameter("name", "Jane")
                .SingleOrDefault();
            Assert.Equal("Jane", emp.Name);
        }

        // using js function
        using (var s = store.OpenSession())
        {
            var emp = s.Advanced.RawQuery<Employee>("declare function check(r) { return r.Name[0] == 'J'} from Employees as e filter check(e)")
                .AddParameter("name", "Jane")
                .SingleOrDefault();
            Assert.Equal("Jane", emp.Name);

            // passing variable to function #1
            emp = s.Advanced.RawQuery<Employee>("declare function check(r) { return r.Name[0] == $prefix} from Employees as e filter check(e)")
                .AddParameter("name", "Jane")
                .AddParameter("prefix", "J")
                .SingleOrDefault();
            Assert.Equal("Jane", emp.Name);

            // passing variable to function #2
            emp = s.Advanced.RawQuery<Employee>("declare function check(r, prefix) { return r.Name[0] == prefix} from Employees as e filter check(e, $prefix)")
                .AddParameter("name", "Jane")
                .AddParameter("prefix", "J")
                .SingleOrDefault();
            Assert.Equal("Jane", emp.Name);
        }


        // with load
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(1, SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    [RavenData(3, SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Sharded)]
    public async Task AsyncCanUseFilterWithCollectionQuery(Options options, long scannedResults)
    {
        using var store = GetDocumentStore(options);
        var data = GetDatabaseItems();
        Insert(store, data, options.DatabaseMode);

        Employee result;
        QueryStatistics stats;

        // simple
        using (var s = store.OpenAsyncSession())
        {
            result = await s.Advanced
                .AsyncDocumentQuery<Employee>()
                .Filter(p => p.Equals(a => a.Name, "Jane"))
                .SingleOrDefaultAsync();
            Assert.Equal("Jane", result.Name);

            result = await s.Query<Employee>()
                .Filter(f => f.Name == "Jane")
                .SingleOrDefaultAsync();
            Assert.Equal("Jane", result.Name);
        }

        using (var s = store.OpenAsyncSession())
        {
            result = await s.Advanced
                .AsyncDocumentQuery<Employee>()
                .Filter(f => f.Equals("Active", false))
                .Statistics(out stats)
                .SingleOrDefaultAsync();
            Assert.Equal("Mark", result.Name);
            Assert.Equal(3, stats.ScannedResults);

            result = await s.Query<Employee>()
                .Filter(f => f.Active == false)
                .Statistics(out stats)
                .SingleOrDefaultAsync();
            Assert.Equal("Mark", result.Name);
            Assert.Equal(3, stats.ScannedResults);
        }

        using (var s = store.OpenAsyncSession())
        {
            result = await s.Advanced
                .AsyncDocumentQuery<Employee>()
                .Filter(f => f.Equals("Active", true), 1)
                .Statistics(out stats)
                .SingleOrDefaultAsync();
            Assert.Equal("Jane", result.Name);
            Assert.Equal(scannedResults, stats.ScannedResults);

            result = await s.Query<Employee>()
                .Filter(f => f.Active == true, 1)
                .Statistics(out stats)
                .SingleOrDefaultAsync();
            Assert.Equal("Jane", result.Name);
            Assert.Equal(scannedResults, stats.ScannedResults);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanFilterWithLoad(Options options)
    {
        using var store = GetDocumentStore(options);
        var data = GetDatabaseItems();
        Insert(store, data, options.DatabaseMode);

        using (var s = store.OpenSession())
        {
            var emp = s.Advanced
                .RawQuery<Employee>("from Employees as e load e.Manager as m filter e.Name = $name and m.Name = $manager")
                .AddParameter("manager", "Jane")
                .AddParameter("name", "Sandra")
                .SingleOrDefault();
            Assert.Equal("Sandra", emp.Name);

            // ensure we filter
            emp = s.Advanced
                .RawQuery<Employee>("from Employees as e load e.Manager as m filter e.Name = $name and m.Name = $manager")
                .AddParameter("manager", "Mark")
                .AddParameter("name", "Sandra")
                .SingleOrDefault();


            Assert.Null(emp);
        }

        // with projections
        using (var s = store.OpenSession())
        {
            var projection = s.Advanced
                .RawQuery<Projection>("from Employees as e load e.Manager as m filter e.Name = $name and m.Name = $manager select e.Name, m.Name as ManagerName")
                .AddParameter("manager", "Jane")
                .AddParameter("name", "Sandra")
                .SingleOrDefault();
            Assert.Equal("Sandra", projection.Name);
            Assert.Equal("Jane", projection.ManagerName);

            // projection via JS
            projection = s.Advanced
                .RawQuery<Projection>(
                    "from Employees as e load e.Manager as m filter e.Name = $name and m.Name = $manager select { Name: e.Name, ManagerName: m.Name}")
                .AddParameter("manager", "Mark")
                .AddParameter("name", "Sandra")
                .SingleOrDefault();

            Assert.Null(projection);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanUseFilterQueryOnMapIndexes(Options options)
    {
        using var store = GetDocumentStore(options);
        var databaseItems = GetDatabaseItems(additional: new() {(new Employee("Frank", "emps/jane", true, 51, new Location(47.623473f, -122.306009f)), "emps/frank")});
        Insert(store, databaseItems, options.DatabaseMode);

        using (var s = store.OpenSession())
        {
            var emp = s.Advanced.RawQuery<Employee>("from Employees where Active = true filter Name = 'Jane'").SingleOrDefault();
            Assert.Equal("Jane", emp.Name);
        }

        using (var s = store.OpenSession())
        {
            var emp = s.Advanced.DocumentQuery<Employee>()
                .WhereEquals(w => w.Active, true).Filter(f =>
                    f.Equals(p => p.Name, "Jane"))
                .SingleOrDefault();
            Assert.Equal("Jane", emp.Name);
        }

        using (var s = store.OpenSession())
        {
            var emp = s.Query<Employee>()
                .Where(w => w.Active)
                .Filter(f => f.Name == "Jane")
                .SingleOrDefault();
            Assert.Equal("Jane", emp.Name);
        }
        using (var s = store.OpenSession())
        {
            var emp = s.Advanced.RawQuery<Employee>("from Employees where exists(Name) filter Age = 51")
                .Statistics(out var stats)
                .SingleOrDefault();
            Assert.Equal("Frank", emp.Name);
            Assert.Equal(4, stats.ScannedResults);
        }
        // scan results
        using (var s = store.OpenSession())
        {
            var emp = s.Advanced.RawQuery<Employee>("from Employees order by Name filter Active = false")
                .Statistics(out var stats)
                .SingleOrDefault();
            Assert.Equal("Mark", emp.Name);
            Assert.Equal(4, stats.ScannedResults);
        }

        using (var s = store.OpenSession())
        {
            var emp = s.Advanced.DocumentQuery<Employee>()
                .OrderBy(ob => ob.Name)
                .Filter(f => f.Equals("Active", false))
                .Statistics(out var stats)
                .SingleOrDefault();
            Assert.Equal("Mark", emp.Name);
            Assert.Equal(4, stats.ScannedResults);
        }

        using (var s = store.OpenSession())
        {
            var emp = s.Query<Employee>()
                .OrderBy(ob => ob.Name)
                .Filter(f => f.Active == false)
                .Statistics(out var stats)
                .SingleOrDefault();
            Assert.Equal("Mark", emp.Name);
            Assert.Equal(4, stats.ScannedResults);
        }


        // scan limit
        using (var s = store.OpenSession())
        {
            var emp = s.Advanced.RawQuery<Employee>("from Employees order by Name desc filter Active = true filter_limit 1")
                .Statistics(out var stats)
                .SingleOrDefault();
            Assert.Equal("Sandra", emp.Name);
            Assert.Equal(1, stats.ScannedResults);
        }

        using (var s = store.OpenSession())
        {
            var emp = s.Advanced.DocumentQuery<Employee>()
                .OrderByDescending("Name")
                .Filter(f => f.Equals(e => e.Active, true), 1)
                .Statistics(out var stats)
                .SingleOrDefault();
            Assert.Equal("Sandra", emp.Name);
            Assert.Equal(1, stats.ScannedResults);
        }

        using (var s = store.OpenSession())
        {
            var emp = s.Query<Employee>()
                .OrderByDescending(x => x.Name)
                .Filter(x => x.Active == true, 1)
                .Statistics(out var stats)
                .SingleOrDefault();
            Assert.Equal("Sandra", emp.Name);
            Assert.Equal(1, stats.ScannedResults);
        }

        // parameters
        using (var s = store.OpenSession())
        {
            var emp = s.Advanced.RawQuery<Employee>("from Employees where Active = $active filter Name = $name")
                .AddParameter("name", "Jane")
                .AddParameter("active", true)
                .SingleOrDefault();
            Assert.Equal("Jane", emp.Name);
        }

        // alias
        using (var s = store.OpenSession())
        {
            var emp = s.Advanced.RawQuery<Employee>("from Employees as e where e.Active = true filter e.Name = $name")
                .AddParameter("name", "Jane")
                .SingleOrDefault();
            Assert.Equal("Jane", emp.Name);
        }


        // using js function
        using (var s = store.OpenSession())
        {
            var emp = s.Advanced.RawQuery<Employee>("declare function check(r) { return r.Name[0] == 'J'} from Employees as e where e.Active = true filter check(e)")
                .AddParameter("name", "Jane")
                .SingleOrDefault();
            Assert.Equal("Jane", emp.Name);

            // passing variable to function #1
            emp = s.Advanced.RawQuery<Employee>("declare function check(r) { return r.Name[0] == $prefix} from Employees as e where e.Active = true filter check(e)")
                .AddParameter("name", "Jane")
                .AddParameter("prefix", "J")
                .SingleOrDefault();
            Assert.Equal("Jane", emp.Name);

            // passing variable to function #2
            emp = s.Advanced.RawQuery<Employee>(
                    "declare function check(r, prefix) { return r.Name[0] == prefix} from Employees as e where e.Active = true filter check(e, $prefix)")
                .AddParameter("name", "Jane")
                .AddParameter("prefix", "J")
                .SingleOrDefault();
            Assert.Equal("Jane", emp.Name);
        }


        // with load
        using (var s = store.OpenSession())
        {
            var emp = s.Advanced.RawQuery<Employee>("from Employees as e where e.Active = $active  load e.Manager as m filter e.Name = $name and m.Name = $manager")
                .AddParameter("manager", "Jane")
                .AddParameter("name", "Sandra")
                .AddParameter("active", true)
                .SingleOrDefault();
            Assert.Equal("Sandra", emp.Name);

            // ensure we filter
            emp = s.Advanced.RawQuery<Employee>("from Employees as e where e.Active = true load e.Manager as m filter e.Name = $name and m.Name = $manager")
                .AddParameter("manager", "Mark")
                .AddParameter("name", "Sandra")
                .SingleOrDefault();

            Assert.Null(emp);
        }

        // with projections
        using (var s = store.OpenSession())
        {
            var projection = s.Advanced
                .RawQuery<Projection>(
                    "from Employees as e where e.Active = true load e.Manager as m filter e.Name = $name and m.Name = $manager select e.Name, m.Name as ManagerName")
                .AddParameter("manager", "Jane")
                .AddParameter("name", "Sandra")
                .SingleOrDefault();
            Assert.Equal("Sandra", projection.Name);
            Assert.Equal("Jane", projection.ManagerName);

            // projection via JS
            projection = s.Advanced
                .RawQuery<Projection>(
                    "from Employees as e where e.Active = true load e.Manager as m filter e.Name = $name and m.Name = $manager select { Name: e.Name, ManagerName: m.Name}")
                .AddParameter("manager", "Mark")
                .AddParameter("name", "Sandra")
                .SingleOrDefault();

            Assert.Null(projection);
        }
        // spatial
        using (var s = store.OpenSession())
        {
            var shape =
                "POLYGON((-122.32246398925781 47.643055992166275,-122.32795715332031 47.62917538239487,-122.33207702636719 47.60904194838943,-122.32109069824219 47.595846873927044,-122.31422424316406 47.594920778814824,-122.30701446533203 47.58959541384278,-122.28538513183594 47.59029005739745,-122.27989196777344 47.620382422330565,-122.28401184082031 47.62454769305083,-122.27645874023438 47.632414521155376,-122.27577209472656 47.6421307328982,-122.29328155517578 47.64536906863988,-122.32246398925781 47.643055992166275))";
            var emp = s.Advanced.RawQuery<Employee>(@"
from Employees 
where spatial.within(spatial.point(Location.Latitude, Location.Longitude), spatial.wkt($wkt))
filter Name = 'Frank'")
                .AddParameter("wkt",
                    shape)
                .SingleOrDefault();
            Assert.Equal("Frank", emp.Name);

            emp = s
                .Query<Employee>()
                .Spatial(f => f.Point(x => x.Location.Latitude, x => x.Location.Longitude), f => f.RelatesToShape(shape, SpatialRelation.Within))
                .Filter(p => p.Name == "Frank")
                .SingleOrDefault();
            Assert.Equal("Frank", emp.Name);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData("from Employees filter spatial.within(spatial.point(Location.Latitude, Location.Longitude), spatial.wkt($wkt))", typeof(RavenException), DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
    [RavenData("from Employees filter MoreLikeThis('emps/jane')", typeof(RavenException), DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
    [RavenData("from Employees filter MoreLikeThis('emps/jane') select suggest(Name, 'jake')", typeof(RavenException), DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
    [RavenData("from Employees filter Age < 10 select facet(Name)", typeof(InvalidQueryException), DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
    public void InvalidFilterQueries(Options options, string q, Type exception)
    {
        using var store = GetDocumentStore();
        var databaseItems = GetDatabaseItems(additional: new() {(new Employee("Frank", "emps/jane", true, 51, new Location(47.623473f, -122.306009f)), "emps/frank")});
        Insert(store, databaseItems, options.DatabaseMode);

        using (var s = store.OpenSession())
        {
            Assert.Throws(exception, () => s.Advanced.RawQuery<Employee>(q)
                .SingleOrDefault());
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void InvalidFilterQueriesInLinq(Options options)
    {
        using var store = GetDocumentStore(options);
        Insert(store, GetDatabaseItems(), options.DatabaseMode);
        {
            using var session = store.OpenSession();
            Assert.Throws(typeof(InvalidOperationException), () =>
            {
                session.Query<Employee>().Filter(f => f.Active == true).Intersect().Filter(f => f.Name == "test").ToList();
            });
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void CanUseFilterQueryOnMapReduce(Options options)
    {
        using var store = GetDocumentStore(options);
        Insert(store, GetDatabaseItems(), options.DatabaseMode);

        Summary summary;
        using (var s = store.OpenSession())
        {
            summary = s.Advanced
                .RawQuery<Summary>("from Employees group by Manager filter Count == 2 select count(), Manager")
                .SingleOrDefault();
            Assert.Equal("emps/jane", summary.Manager);
            Assert.Equal(2, summary.Count);


            summary = s.Advanced
                .DocumentQuery<Employee>()
                .GroupBy("Manager")
                .Filter(b => b.Equals("Count", 2))
                .SelectKey("Manager")
                .SelectCount("Count")
                .OfType<Summary>()
                .SingleOrDefault();
            Assert.Equal("emps/jane", summary.Manager);
            Assert.Equal(2, summary.Count);

            summary = s.Query<Employee>()
                .GroupBy(p => p.Manager)
                .Filter(f => f.Count() == 2)
                .Select(p =>
                    new Summary { Count = p.Count(), Manager = p.Key })
                .SingleOrDefault();

            Assert.Equal("emps/jane", summary.Manager);
            Assert.Equal(2, summary.Count);
        }

        // parameters
        using (var s = store.OpenSession())
        {
            summary = s.Advanced
                .RawQuery<Summary>("from Employees group by Manager filter Count == $count select count(), Manager")
                .AddParameter("count", 2)
                .SingleOrDefault();
            Assert.Equal("emps/jane", summary.Manager);
            Assert.Equal(2, summary.Count);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public async Task AsyncCanUseFilterQueryOnMapReduce(Options options)
    {
        using var store = GetDocumentStore(options);
        Insert(store, GetDatabaseItems(), options.DatabaseMode);

        Summary summary;
        using (var s = store.OpenAsyncSession())
        {
            summary = await s.Advanced
                .AsyncDocumentQuery<Employee>()
                .GroupBy("Manager")
                .Filter(b => b.Equals("Count", 2))
                .SelectKey("Manager")
                .SelectCount("Count")
                .OfType<Summary>()
                .SingleOrDefaultAsync();
            Assert.Equal("emps/jane", summary.Manager);
            Assert.Equal(2, summary.Count);

            summary = await s.Query<Employee>()
                .GroupBy(p => p.Manager)
                .Filter(f => f.Count() == 2)
                .Select(p =>
                    new Summary { Count = p.Count(), Manager = p.Key })
                .SingleOrDefaultAsync();

            Assert.Equal("emps/jane", summary.Manager);
            Assert.Equal(2, summary.Count);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void ExtendedLinqTest(Options options)
    {
        using var store = GetDocumentStore(options);
        var data = GetDatabaseItems();
        Insert(store, data, options.DatabaseMode);

        {
            //concat
            using var session = store.OpenSession();
            var q = session.Query<Employee>().Filter(f => f.Active == true).Filter(f => f.Name == "Jane").SingleOrDefault();
            Assert.NotNull(q);
            Assert.Equal("Jane", q.Name);
        }
        {
            //between
            using var session = store.OpenSession();
            var q = session.Query<Employee>().Filter(f => f.Age >= 30 && f.Age <= 40).ToList();
            Assert.NotNull(q);
            Assert.Equal(2, q.Count);
            Assert.InRange(q[0].Age, 30, 40);
            Assert.InRange(q[1].Age, 30, 40);
        }
        {
            //where + filter
            using var session = store.OpenSession();
            var q = session.Query<Employee>().Where(w => w.Age >= 30).Filter(f => f.Age <= 40).ToList();
            Assert.NotNull(q);
            Assert.Equal(2, q.Count);
            Assert.InRange(q[0].Age, 30, 40);
            Assert.InRange(q[1].Age, 30, 40);
        }
        {
            //no sense, just for RQL check
            using var session = store.OpenSession();
            var q = session.Query<Employee>().Where(w => (w.Age >= 30) && (w.Age >= 30 || w.Age > 30)).Filter(f => f.Age <= 40 && (f.Age <= 40 || f.Age < 40)).ToList();
            Assert.NotNull(q);
            Assert.Equal(2, q.Count);
            Assert.InRange(q[0].Age, 30, 40);
            Assert.InRange(q[1].Age, 30, 40);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void CannotUseFacetWithFilter(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            new BlogIndex().Execute(store);
            var facets = new List<Facet> { new Facet { FieldName = "Tags", Options = new FacetOptions { TermSortMode = FacetTermSortMode.CountDesc } } };

            using (var session = store.OpenSession())
            {
                session.Store(new FacetSetup() { Facets = facets, Id = "facets/BlogFacets" });
                var post1 = new BlogPost { Title = "my first blog", Tags = new List<string>() { "news", "funny" } };
                session.Store(post1);
                var post2 = new BlogPost { Title = "my second blog", Tags = new List<string>() { "lame", "news" } };
                session.Store(post2);
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var q = session.Query<BlogPost, BlogIndex>().Filter(p => p.Tags.Contains("news"));

                Assert.Throws(typeof(InvalidQueryException), () => q.AggregateUsing("facets/BlogFacets").Execute());
            }
        }
    }

    private List<(Employee Entity, string Id)> GetDatabaseItems(List<(Employee Entity, string Id)> additional = null)
    {
        return new(additional ?? new())
        {
            (new Employee("Jane", null, true, 20), "emps/jane"),
            (new Employee("Mark", "emps/jane", false, 33), "emps/mark"),
            (new Employee("Sandra", "emps/jane", true, 35), "emps/sandra"),
        };
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void Timings(Options options)
    {
        var timings = GetTimings(options);

        Assert.True(timings.Timings[nameof(QueryTimingsScope.Names.Query)].Timings[nameof(QueryTimingsScope.Names.Filter)]
            .DurationInMs >= 0);
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Sharded)]
    public void TimingsSharded(Options options)
    {
        var timings = GetTimings(options);

        var shards = timings.Timings[nameof(QueryTimingsScope.Names.Query)].Timings[nameof(QueryTimingsScope.Names.Execute)];
        var count = 0;
        foreach (var shard in shards.Timings)
        {
            var queryTimings = shard.Value.Timings[nameof(QueryTimingsScope.Names.Query)];
            if (queryTimings.Timings.TryGetValue(nameof(QueryTimingsScope.Names.Filter), out var filterTimings) == false)
                continue;

            count++;
            Assert.True(filterTimings.DurationInMs >= 0);
        }

        Assert.Equal(2, count);
    }

    private QueryTimings GetTimings(Options options)
    {
        using var store = GetDocumentStore(options);
        var data = GetDatabaseItems();
        Insert(store, data, options.DatabaseMode);

        using (var session = store.OpenSession())
        {
            var query =
                session.Advanced.RawQuery<Employee>(
                        "declare function check(r, prefix) { return r.Name[0] == prefix} from Employees as e where e.Active = true filter check(e, $prefix) include timings()")
                    .AddParameter("name", "Jane")
                    .AddParameter("prefix", "J")
                    .Timings(out var timings);

            var queryResult = query.ToList();
            Assert.Equal("Jane", queryResult[0].Name);

            return timings;
        }
    }

    private static void Insert(DocumentStore store, List<(Employee Entity, string Id)> data, RavenDatabaseMode databaseMode)
    {
        switch (databaseMode)
        {
            case RavenDatabaseMode.Single:
                using (var bulkInsert = store.BulkInsert())
                {
                    foreach ((Employee entity, string id) in data)
                    {
                        bulkInsert.Store(entity, id);
                    }
                }
                
                break;
            case RavenDatabaseMode.Sharded:
                foreach ((Employee entity, string id) in data)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(entity, id);
                        session.SaveChanges();
                    }
                }
                break;
        }
    }

    private record Location(float Latitude, float Longitude);

    private record Employee(string Name, string Manager, bool Active, int Age, Location Location = null);

    private record Projection(string Name, string ManagerName);

    private class Summary
    {
        public int Count { get; set; }
        public string Manager { get; set; }
    }

    private class BlogPost
    {
        public string Title { get; set; }
        public List<string> Tags { get; set; }
    }

    private class BlogIndex : AbstractIndexCreationTask<BlogPost>
    {
        public BlogIndex()
        {
            Map = blogs => from b in blogs
                           select new { Tags = b.Tags };
            Store("Tags", FieldStorage.Yes);
            Index("Tags", FieldIndexing.Exact);
        }
    }
}
