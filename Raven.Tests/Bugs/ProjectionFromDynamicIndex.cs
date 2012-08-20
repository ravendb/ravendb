using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class ProjectionFromDynamicIndex : LocalClientTest
	{
		[Fact]
		public void ShouldWork()
		{
			using(var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Post{Title = "ayende"});
					session.SaveChanges();
				}

				using(var session =store.OpenSession())
				{
					var postReference = session.Query<Post>().Customize(x=>x.WaitForNonStaleResults())
						.Select(p => new {Id = p.Id, p.Title})
						.First();

					Assert.Equal("ayende", postReference.Title);
					Assert.Equal("posts/1", postReference.Id);
				}
			}
		}

		public class Post
		{
			public string Id { get; set; }
			public string Title { get; set; }
		}
	}
}