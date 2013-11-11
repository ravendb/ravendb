using System;
using System.Linq;
using Xunit;
using Raven.Client.Document;

namespace Raven.Tests.Bugs.Iulian
{
	public class CanReadEntityWithUrlId : RemoteClientTest
	{
		public class Event
		{
			public string Id { get; set; }
			public string Tag { get; set; }
		}

		//[Fact]
		[TimeBombedFact(2013, 12, 31)]
		public void Can_Load_entities_with_id_containing_url()
		{
			var id = @"mssage@msmq://local/Sample.AppService";

			using (GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				using (var s = store.OpenSession())
				{
					Event e = new Event { Id = id, Tag = "tag" };
					s.Store(e);
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					Event loaded = s.Query<Event>().Where(e => e.Id == id).Single();
					// this passes
					Assert.NotNull(loaded);
					Assert.Equal("tag",loaded.Tag);
				}

				using (var s = store.OpenSession())
				{
					Event loaded = s.Load<Event>(id);
					// this fails
					Assert.NotNull(loaded);
					Assert.Equal("tag", loaded.Tag);
				}
			}
		}        
	}
}
