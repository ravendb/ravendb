using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Vlad : RavenTest
	{
		[Fact]
		public void WillOnlyGetPost2Once()
		{
			using (EmbeddableDocumentStore store = NewDocumentStore())
			{
				new Post_ByTag().Execute(store);
				using (IDocumentSession session = store.OpenSession())
				{
					session.Store(new Post
					{
						Title = "Post1",
						Tags = new[] {"cloud", "azure"}
					});
					
					session.Store(new Post
					{
						Title = "Post2",
						Tags = new[] { "amazon", "cloud", "cloud" }
					}); 
					session.Store(new Post
					{
						Title = "Post3",
						Tags = new[] { "events" }
					});
					session.Store(new Post
					{
						Title = "Post4",
						Tags = new[] { "raven", "db", "cloud" }
					});

					session.SaveChanges();
				}
				using (IDocumentSession session = store.OpenSession())
				{
					int pageSize = 2;
					int pageNumber = 0;
					int recordsToSkip = 0;

					var posts = new List<Post>();

					RavenQueryStatistics stat;
					while (true)
					{
						List<Post> results = session.Query<Post_ByTag.Result, Post_ByTag>()
							.Customize(x => x.WaitForNonStaleResults())
							.OrderBy(x=>x.Title)
							.Statistics(out stat)
							.Where(x => x.Tag == "cloud")
							.Take(pageSize)
							.Skip(recordsToSkip)
							.As<Post>()
							.ToList();

						posts.AddRange(results);

						if (results.Count < pageSize)
							break;

						pageNumber++;
						recordsToSkip = pageSize*pageNumber + stat.SkippedResults;
					}

					Assert.Equal(3, posts.Count);
				}
			}
		}

		#region Nested type: Post

		public class Post_ByTag : AbstractIndexCreationTask<Post>
		{
			public Post_ByTag()
			{
				Map = posts => from post in posts
				               from Tag in post.Tags
				               select new {Tag, post.Title};
			}

			public class Result
			{
				public string Tag { get; set; }
				public string Title { get; set; }
			}
		}

		public class Post
		{
			public string Title { get; set; }
			public string[] Tags { get; set; }

			public override string ToString()
			{
				return string.Format("Title: {0}", Title);
			}
		}

		#endregion
	}
}