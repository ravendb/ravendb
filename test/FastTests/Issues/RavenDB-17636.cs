using System;
using System.Linq;
using NuGet.Protocol.Plugins;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_17636 : RavenTestBase
    {
        public RavenDB_17636(ITestOutputHelper output) : base(output)
        {
        }

        private record Location(float Latitude, float Longitude);
        private record Employee(string Name, string Manager, bool Active, Location Location = null);

        private record Projection(string Name, string ManagerName);

        private record Summary(int Count, string Manager);

        [Fact]
        public void CanUseFilterWithCollectionQuery()
        {
            using var store = GetDocumentStore();

            using (var s = store.OpenSession())
            {
                s.Store(new Employee("Jane", null, true), "emps/jane");
                s.Store(new Employee("Mark", "emps/jane", false), "emps/mark");
                s.Store(new Employee("Sandra", "emps/jane", true), "emps/sandra");
                s.SaveChanges();
            }

            
            // raw
            using (var s = store.OpenSession())
            {
                var emp = s.Advanced.RawQuery<Employee>("from Employees filter Name = 'Jane'").SingleOrDefault();
                Assert.Equal("Jane", emp.Name);
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
                var emp = s.Advanced.RawQuery<Employee>("from Employees as e filter e.Name = $name")
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
            using (var s = store.OpenSession())
            {
                var emp = s.Advanced.RawQuery<Employee>("from Employees as e load e.Manager as m filter e.Name = $name and m.Name = $manager")
                    .AddParameter("manager", "Jane")
                    .AddParameter("name", "Sandra")
                    .SingleOrDefault();
                Assert.Equal("Sandra", emp.Name);
                
                // ensure we filter
                emp = s.Advanced.RawQuery<Employee>("from Employees as e load e.Manager as m filter e.Name = $name and m.Name = $manager")
                    .AddParameter("manager", "Mark")
                    .AddParameter("name", "Sandra")
                    .SingleOrDefault();

                Assert.Null(emp);
            }
            
            // with projections
            using (var s = store.OpenSession())
            {
                var projection = s.Advanced.RawQuery<Projection>("from Employees as e load e.Manager as m filter e.Name = $name and m.Name = $manager select e.Name, m.Name as ManagerName")
                    .AddParameter("manager", "Jane")
                    .AddParameter("name", "Sandra")
                    .SingleOrDefault();
                Assert.Equal("Sandra", projection.Name);
                Assert.Equal("Jane", projection.ManagerName);
                
                // projection via JS
                projection = s.Advanced.RawQuery<Projection>("from Employees as e load e.Manager as m filter e.Name = $name and m.Name = $manager select { Name: e.Name, ManagerName: m.Name}")
                    .AddParameter("manager", "Mark")
                    .AddParameter("name", "Sandra")
                    .SingleOrDefault();

                Assert.Null(projection);
            }
        }

        [Fact]
        public void CanUseFilterQueryOnMapIndexes()
        {
            using var store = GetDocumentStore();

            using (var s = store.OpenSession())
            {
                s.Store(new Employee("Jane", null, true), "emps/jane");
                s.Store(new Employee("Mark", "emps/jane", false), "emps/mark");
                s.Store(new Employee("Sandra", "emps/jane", true), "emps/sandra");
                s.Store(new Employee("Frank", "emps/jane", true, new Location(47.623473f, -122.306009f)), "emps/frank");
                s.SaveChanges();
                
            }

            // raw
            using (var s = store.OpenSession())
            {
                var emp = s.Advanced.RawQuery<Employee>("from Employees where Active = true filter Name = 'Jane'").SingleOrDefault();
                Assert.Equal("Jane", emp.Name);
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
                WaitForUserToContinueTheTest(store);
                Assert.Equal("Jane", emp.Name);
                
                // passing variable to function #1
                emp = s.Advanced.RawQuery<Employee>("declare function check(r) { return r.Name[0] == $prefix} from Employees as e where e.Active = true filter check(e)")
                    .AddParameter("name", "Jane")
                    .AddParameter("prefix", "J")
                    .SingleOrDefault();
                Assert.Equal("Jane", emp.Name);
                
                // passing variable to function #2
                emp = s.Advanced.RawQuery<Employee>("declare function check(r, prefix) { return r.Name[0] == prefix} from Employees as e where e.Active = true filter check(e, $prefix)")
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
                var projection = s.Advanced.RawQuery<Projection>("from Employees as e where e.Active = true load e.Manager as m filter e.Name = $name and m.Name = $manager select e.Name, m.Name as ManagerName")
                    .AddParameter("manager", "Jane")
                    .AddParameter("name", "Sandra")
                    .SingleOrDefault();
                Assert.Equal("Sandra", projection.Name);
                Assert.Equal("Jane", projection.ManagerName);
                
                // projection via JS
                projection = s.Advanced.RawQuery<Projection>("from Employees as e where e.Active = true load e.Manager as m filter e.Name = $name and m.Name = $manager select { Name: e.Name, ManagerName: m.Name}")
                    .AddParameter("manager", "Mark")
                    .AddParameter("name", "Sandra")
                    .SingleOrDefault();

                Assert.Null(projection);
            }
            
            // spatial
            using (var s = store.OpenSession())
            {
                var emp = s.Advanced.RawQuery<Employee>(@"
from Employees 
where spatial.within(spatial.point(Location.Latitude, Location.Longitude), spatial.wkt($wkt))
filter Name = 'Frank'")
                    .AddParameter("wkt", "POLYGON((-122.32246398925781 47.643055992166275,-122.32795715332031 47.62917538239487,-122.33207702636719 47.60904194838943,-122.32109069824219 47.595846873927044,-122.31422424316406 47.594920778814824,-122.30701446533203 47.58959541384278,-122.28538513183594 47.59029005739745,-122.27989196777344 47.620382422330565,-122.28401184082031 47.62454769305083,-122.27645874023438 47.632414521155376,-122.27577209472656 47.6421307328982,-122.29328155517578 47.64536906863988,-122.32246398925781 47.643055992166275))")
                    .SingleOrDefault();
                Assert.Equal("Frank", emp.Name);
            }
        }

        [Theory]
        [InlineData("from Employees filter spatial.within(spatial.point(Location.Latitude, Location.Longitude), spatial.wkt($wkt))", typeof(RavenException))]
        [InlineData("from Employees filter MoreLikeThis('emps/jane')", typeof(RavenException))]
        [InlineData("from Employees filter MoreLikeThis('emps/jane') select suggest(Name, 'jake')", typeof(RavenException))]
        [InlineData("from Employees filter Age < 10 select facet(Name)", typeof(InvalidQueryException))]

        public void InvalidFilterQueries(string q, Type exception)
        {
            using var store = GetDocumentStore();
            
            using (var s = store.OpenSession())
            {
                s.Store(new Employee("Jane", null, true), "emps/jane");
                s.Store(new Employee("Mark", "emps/jane", false), "emps/mark");
                s.Store(new Employee("Sandra", "emps/jane", true), "emps/sandra");
                s.Store(new Employee("Frank", "emps/jane", true, new Location(47.623473f, -122.306009f)), "emps/frank");
                s.SaveChanges();
            }
            
            using (var s = store.OpenSession())
            {
                 Assert.Throws(exception, () => s.Advanced.RawQuery<Employee>(q)
                    .SingleOrDefault());
            }
        }
        
        [Fact]
        public void CanUseFilterQueryOnMapReduce()
        {
            using var store = GetDocumentStore();

            using (var s = store.OpenSession())
            {
                s.Store(new Employee("Jane", null, true), "emps/jane");
                s.Store(new Employee("Mark", "emps/jane", false), "emps/mark");
                s.Store(new Employee("Sandra", "emps/jane", true), "emps/sandra");
                s.SaveChanges();
                
            }

            // raw
            using (var s = store.OpenSession())
            {
                var summary = s.Advanced.RawQuery<Summary>("from Employees group by Manager filter Count == 2 select count(), Manager").SingleOrDefault();
                Assert.Equal("emps/jane", summary.Manager);
                Assert.Equal(2, summary.Count);
            }
            
            // parameters
            using (var s = store.OpenSession())
            {
                var summary = s.Advanced.RawQuery<Summary>("from Employees group by Manager filter Count == $count select count(), Manager")
                    .AddParameter("count", 2)
                    .SingleOrDefault();
                Assert.Equal("emps/jane", summary.Manager);
                Assert.Equal(2, summary.Count);
            }
        }
        
    }
}
