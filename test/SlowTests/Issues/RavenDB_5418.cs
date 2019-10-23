using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_5418 : RavenTestBase
    {
        public RavenDB_5418(ITestOutputHelper output) : base(output)
        {
        }

        private class Order
        {
        }

        private class Employee
        {
        }

        private class Company
        {
        }

        [Fact]
        public void WillNotCorruptStat()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 15; i++)
                    {
                        session.Store(new Order());
                        session.Store(new Employee());
                        session.Store(new Company());
                    }
                    session.SaveChanges();
                }
            }
        }
    }
}
