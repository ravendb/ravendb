using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17689 : RavenTestBase
    {
        public RavenDB_17689(ITestOutputHelper output) : base(output)
        {
        }

        private class Employee
        {
            public Employee(string FirstName, string Manager)
            {
                this.FirstName = FirstName;
                this.Manager = Manager;
            }

            public string FirstName { get; }
            public string Manager { get; }

        }

        private class Projection
        {
            public Projection(string e, string r)
            {
                this.e = e;
                this.r = r;
            }

            public string e { get; }
            public string r { get; }

        }

        [Fact]
        public void CanQueryOnProjectionsThatWeAlsoReturn()
        {
            using var store = GetDocumentStore();

            using(var session = store.OpenSession())
            {
                session.Store(new Employee("Smith", "emps/jane"));
                session.Store(new Employee("Jane", null), "emps/jane");
                session.Store(new Employee("Sandra", "emps/jane"));
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var emps = session.Advanced.RawQuery<Projection>(@"
from Employees  as e
load e.Manager as r
select e.FirstName as e, r.FirstName as r")
                    .ToList();
                WaitForUserToContinueTheTest(store);
                Assert.Equal(3, emps.Count);
                int notNull = 0;
                foreach (var emp in emps)
                {
                    if (emp.r != null)
                        notNull++;
                }
                Assert.Equal(2, notNull);


            }
        }
    }
}
