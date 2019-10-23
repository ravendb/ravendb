using System.Linq;
using FastTests;
using Orders;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8746 : RavenTestBase
    {
        public RavenDB_8746(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_use_alias_in_group_by()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "joe",
                        Address = new Address
                        {
                            City = "Seattle"
                        }
                    });

                    session.Store(new Employee
                    {
                        FirstName = "joe",
                    });

                    session.SaveChanges();

                    var names = session.Advanced.RawQuery<Result>("from Employees as e group by e.FirstName select e.FirstName, count()").ToList();

                    Assert.Equal(1, names.Count);
                    Assert.Equal("joe", names[0].FirstName);
                    Assert.Equal(2, names[0].Count);

                    var results = session.Advanced.RawQuery<Result>(@"from Employees as e
group by e.Address.City
where e.Address.City != null
select count(), key() as City").ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal("Seattle", results[0].City);
                    Assert.Equal(1, results[0].Count);
                }
            }
        }

        [Fact]
        public void Can_use_alias_in_sum_by()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Product
                    {
                        Supplier = "suppliers/1",
                        UnitsInStock = 10
                    });

                    session.Store(new Product
                    {
                        Supplier = "suppliers/1",
                        UnitsInStock = 20
                    });

                    session.SaveChanges();

                    var names = session.Advanced.RawQuery<Result>("from Products as p group by p.Supplier select key(), sum(p.UnitsInStock)").ToList();

                    Assert.Equal(1, names.Count);
                    Assert.Equal("suppliers/1", names[0].Supplier);
                    Assert.Equal(30, names[0].UnitsInStock);
                }
            }
        }

        private class Result
        {
            public string FirstName { get; set; }

            public int Count { get; set; }

            public int UnitsInStock { get; set; }

            public string Supplier { get; set; }

            public string City { get; set; }
        }
    }
}
