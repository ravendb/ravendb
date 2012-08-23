using System;
using System.Linq;
using Raven.Client;
using Raven.Tests.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class RavenDBQuery : RavenTest
	{
		[Fact]
		public void CanPerformQueryOnParameter()
		{
			using(var store = NewDocumentStore())
			{
				Tag("test", store.OpenSession());
			}
		}

		public void Tag(string slug, IDocumentSession session)
		{

			var postsQuery = from post in session.Query<TagCloud.Post>()
							 where post.Tags.Any(postTag => postTag == slug)
							 select post;

			postsQuery.ToList();
		}
	}
}