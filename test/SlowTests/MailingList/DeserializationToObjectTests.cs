using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class DeserializationToObjectTests : RavenTestBase
    {
        [Fact(Skip = "RavenDB-6124")]
        public void Query_GivenDbWithComplexObjects_ShouldDeserializePropertiesToOriginalType()
        {
            // Arrange
            using (var documentStore = GetDocumentStore())
            {

                using (var session = documentStore.OpenSession())
                {
                    session.Store(new EventContainer
                    { AggregateId = "orders/1", Event = new OrderItemAdded { AggregateId = "orders/1", Sku = "HDD", Quantity = 1 } });
                    session.Store(new EventContainer
                    { AggregateId = "orders/1", Event = new OrderItemAdded { AggregateId = "orders/1", Sku = "KBD", Quantity = 1 } });
                    session.SaveChanges();
                }

                // Act
                object evt;
                using (var session = documentStore.OpenSession())
                {
                    evt = (from ec in session.Query<EventContainer>()
                           where ec.AggregateId == "orders/1"
                           select ec.Event).ToList().First();
                }

                // Assert
                Assert.IsAssignableFrom<OrderItemAdded>(evt);
            }

        }

        private class EventContainer
        {
            public string AggregateId { get; set; }
            public object Event { get; set; }
        }

        private class OrderItemAdded
        {
            public string AggregateId { get; set; }
            public string Sku { get; set; }
            public uint Quantity { get; set; }
        }
    }
}
