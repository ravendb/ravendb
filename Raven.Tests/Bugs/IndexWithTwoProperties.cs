using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class IndexWithTwoProperties : LocalClientTest
	{
		[Fact]
		public void CanCreateIndexByTwoProperties()
		{
			using (var store = NewDocumentStore())
			using (var session = store.OpenSession())
			{
				session.Store(new Foo { Id = "1", Value = "foo" });


				session.Store(new Foo { Id = "2", Value = "bar" });


				session.SaveChanges();

				store.DatabaseCommands.PutIndex(
					"FeedSync/TwoProperties",
					new IndexDefinition<Foo>


					{
						Map = ids => from id in ids
										select new { id.Id, id.Value },
					},
					true);
			}
		}

		public class Foo 
		{
			public string Id { get; set; }
			public string Value { get; set; }
		}
	}
}
