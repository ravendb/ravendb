using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.Iulian
{
	public class CanReadEntityWithUrlId : RavenTest
	{
		public class Event
		{
			public string Id { get; set; }
			public string Tag { get; set; }
		}

		[Fact]
		public void Can_Load_entities_with_id_containing_url()
		{
			var id = @"mssage@msmq://local/Sample.AppService";

			using (var server = GetNewServer(requestedStorage:"esent"))
			using (var store = NewRemoteDocumentStore(ravenDbServer: server))
			{
				using (var s = store.OpenSession())
				{
					var e = new Event { Id = id, Tag = "tag" };
					s.Store(e);
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var loaded = s.Query<Event>().Single(e => e.Id == id);
					// this passes
					Assert.NotNull(loaded);
					Assert.Equal("tag",loaded.Tag);
				}

				using (var s = store.OpenSession())
				{
					var loaded = s.Load<Event>(id);
					// this fails
					Assert.NotNull(loaded);
					Assert.Equal("tag", loaded.Tag);
				}
			}
		}        
	}
}