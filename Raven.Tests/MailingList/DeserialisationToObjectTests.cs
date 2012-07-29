using System.Linq;
using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class DeserialisationToObjectTests : RavenTest
	{
		[Fact]
		public void Query_GivenDbWithComplexObjects_ShouldDeserialisePropertiesToOriginalType()
		{
			// Arrange
			using (var documentStore = NewDocumentStore())
			{

				using (var session = documentStore.OpenSession())
				{
					session.Store(new EventContainer
					{AggregateId = "orders/1", Event = new OrderItemAdded {AggregateId = "orders/1", Sku = "HDD", Quantity = 1}});
					session.Store(new EventContainer
					{AggregateId = "orders/1", Event = new OrderItemAdded {AggregateId = "orders/1", Sku = "KBD", Quantity = 1}});
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

		public class EventContainer
		{
			public string AggregateId { get; set; }
			public object Event { get; set; }
		}

		[System.CLSCompliant(isCompliant:false)]
		public class OrderItemAdded
		{
			public string AggregateId { get; set; }
			public string Sku { get; set; }
			public uint Quantity { get; set; }
		}
	}
}