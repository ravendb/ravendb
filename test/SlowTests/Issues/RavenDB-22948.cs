using System.Linq;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_22948 : ReplicationTestBase
    {
        public RavenDB_22948(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void RavenDB_22948_Fail()
        {
            // Arrange
            var testDocumentStore = GetDocumentStore();

            using (var rs = testDocumentStore.OpenSession())
            {
                // Act
                var customer = new Customer() { Name = "My Customer" };
                rs.Store(customer);

                var order = new Order() { CustomerId = customer.Id, TotalPrice = 100.00 };
                rs.Store(order);
                rs.SaveChanges();

                var customerWithPetName = rs
                    .Query<Order>()
                    .Select(x => new CustomerWithPet()
                    {
                        Customer = rs.Load<Customer>(x.CustomerId),
                        Pet = "My Pet"
                    })
                    .SingleOrDefault();

                // Assert
                Assert.Equal(customer.Id, customerWithPetName.Customer.Id);
            }
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void RavenDB_22948_Pass()
        {
            var testDocumentStore = GetDocumentStore();

            using (var rs = testDocumentStore.OpenSession())
            {
                // Act
                var customer = new Customer() { Name = "My Customer" };
                rs.Store(customer);

                var order = new Order() { CustomerId = customer.Id, TotalPrice = 100.00 };
                rs.Store(order);
                rs.SaveChanges();

                var qryorder = rs
                    .Query<Order>()
                    .Include(o => o.CustomerId)
                    .Where(x => x.Id == order.Id)
                    .SingleOrDefault();

                Customer customerret = rs
                    .Load<Customer>(qryorder.CustomerId);

                var custwithpet = new CustomerWithPet()
                {
                    Customer = customerret,
                    Pet = "My Pet"
                };

                // Assert
                Assert.Equal(customer.Id, custwithpet.Customer.Id);
            }
        }

        public class CustomerWithPet
        {
            public Customer Customer { get; set; }

            public string Pet { get; set; }
        }

        public class Order : EntityWithId
        {
            public string CustomerId { get; set; }

            public double TotalPrice { get; set; }
        }

        public class Customer : EntityWithId
        {

            public string Name { get; set; }
        }

        public static int i;
        public abstract class EntityWithId
        {
            public EntityWithId()
            {
                Id = GenerateId();
            }
            public string Id { get; set; }

            public virtual string GenerateId()
            {
                i++;
                return GetType().Name + "-" + i;
            }
        }
    }
}
