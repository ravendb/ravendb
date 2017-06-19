using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5418 : RavenTestBase
    {
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