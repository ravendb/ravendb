using Xunit;
using Raven.Json.Linq;

namespace Raven.Tests.Bugs.Entities
{
	public class CanSaveUpdateAndRead_Local : RavenTest
	{
		[Fact]
		public void Can_read_entity_name_after_update()
		{
			using(var store = NewDocumentStore())
			{
				using(var s =store.OpenSession())
				{
					s.Store(new Event {Happy = true});
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					s.Load<Event>("events/1").Happy = false;
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var e = s.Load<Event>("events/1");
					var entityName = s.Advanced.GetMetadataFor(e)["Raven-Entity-Name"].Value<string>();
					Assert.Equal("Events", entityName);
				}
			}
		}

		public class Event
		{
			public string Id { get; set; }
			public bool Happy { get; set; }
		}
	}
}