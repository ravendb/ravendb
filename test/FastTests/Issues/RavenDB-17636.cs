using System.Linq;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_17636 : RavenTestBase
    {
        public RavenDB_17636(ITestOutputHelper output) : base(output)
        {
        }

        private record Employee(string Name, string Manager, bool Active);

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
