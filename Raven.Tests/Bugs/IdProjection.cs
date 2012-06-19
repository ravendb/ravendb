using Raven.Client;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class IdProjection : RavenTest
	{
		private const string SpecialId = "BlogPost/CaseSensitive";
		private readonly IDocumentStore store;

		public IdProjection()
		{
			store = NewDocumentStore();

			using (var session = store.OpenSession())
			{
				session.Store(new BlogPost { Id = SpecialId });
				session.SaveChanges();
			}
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void WhenProjectingItFails()
		{
			using (var session = store.OpenSession())
			{
				var posts = session.Query<BlogPost>()
					.Customize(x => x.WaitForNonStaleResultsAsOfNow())
					.Select(post => new BlogPostSummary {Id = post.Id})
					.ToArray();

				Assert.Equal(SpecialId, posts.First().Id);
			}
		}

		[Fact]
		public void WhenQueryThisWorks()
		{
			using (var session = store.OpenSession())
			{
				var posts = session.Query<BlogPost>()
					.Customize(x => x.WaitForNonStaleResultsAsOfNow())
					.ToArray();

				Assert.Equal(SpecialId, posts.First().Id);

			}
		}

		private class BlogPost
		{
			public string Id { get; set; }
		}

		private class BlogPostSummary
		{
			public string Id { get; set; }
		}
	}
}