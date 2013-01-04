using System.Collections.Generic;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class TypeNameHandlingWithCollectionsTest : RavenTest
	{
		public class Item
		{
			[JsonProperty(TypeNameHandling = TypeNameHandling.All)]
			public object Payload { get; set; }
		}

		[Fact]
		public void ShouldWork()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Payload = new List<string> { "Oren" }
					});

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var payload = session.Load<Item>("items/1").Payload as List<string>;

					Assert.Equal("Oren", payload.First());
				}
			}
		}
	}
}