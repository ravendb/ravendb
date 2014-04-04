using System.Linq;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Tests.Bugs.QueryOptimizer;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Querying
{
	public class SkipDuplicates : RavenTest
	{
		[Fact]
		public void WillSkipDuplicates()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex(
					"BlogPosts/PostsCountByTag",
					new IndexDefinitionBuilder<BlogPost>
					{
						Map = posts => from post in posts
							from tag in post.Tags
							select new {Tag = tag}
					});

				using (var session = store.OpenSession())
				{
					session.Store(new BlogPost
					{
						Tags = new []{"Daniel", "Oren"}
					});
					session.SaveChanges();

					WaitForIndexing(store);

					var result = session.Query<BlogPost>("BlogPosts/PostsCountByTag").ToList();
					Assert.Equal(1, result.Count);
				}
			}
		}

		[Fact]
		public void WillNotSkipDuplicates()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex(
					"BlogPosts/PostsCountByTag",
					new IndexDefinitionBuilder<BlogPost>
					{
						Map = posts => from post in posts
									   from tag in post.Tags
									   select new { Tag = tag }
					});

				using (var session = store.OpenSession())
				{
					session.Store(new BlogPost
					{
						Tags = new[] { "Daniel", "Oren" }
					});
					session.SaveChanges();

					WaitForIndexing(store);

					var result = store.DocumentDatabase.Queries.Query("BlogPosts/PostsCountByTag", new IndexQuery{SkipDuplicateChecking = true}, CancellationToken.None);
					Assert.Equal(2, result.Results.Count);
				}
			}
		}
	}
}