using Newtonsoft.Json;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class TypeNameHandlingTest : RavenTest
	{
		public class Item
		{
			[JsonProperty(TypeNameHandling = TypeNameHandling.All)]
			public object Payload { get; set; }
		}

		public class Payload1
		{
			public string Name { get; set; }
		}

		[Fact]
		public void ShouldWork()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Payload = new Payload1
						{
							Name = "Oren"
						}
					});
					session.SaveChanges();
				}

				using(var session = store.OpenSession())
				{
					var payload = session.Load<Item>("items/1").Payload as Payload1;

					Assert.Equal("Oren", payload.Name);
				}
			}
		}
	}
}