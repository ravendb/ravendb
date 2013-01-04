using System.Collections.Generic;
using Raven.Imports.Newtonsoft.Json;
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
		public void ShouldWorkObject()
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

		[Fact]
		public void ShouldWorkList()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Payload = new List<string>{"ayende", "rahien"}
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					dynamic o = session.Load<Item>("items/1").Payload;
					var payload = (List<string>)o;

					Assert.Equal(new List<string>{"ayende", "rahien"}, payload);
				}
			}
		}
	}
}