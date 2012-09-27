using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Shard.BlogModel
{
	public class CanMapReduce : ShardingScenario
	{
		[Fact]
		public void CanDoMapReduceOnAllShards()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				session.Store(new Post { Title = "Item 1", VotesUpCount = 2 });
				session.Store(new Post { Title = "Item 2", VotesUpCount = 3 });
				session.Store(new Post { Title = "Item 3", VotesUpCount = 4 });
				session.Store(new Post { Title = "Item 4", VotesUpCount = 1 });
				session.SaveChanges();
			}

			new TotalVotesUp().Execute(ShardedDocumentStore);
			using (var session = ShardedDocumentStore.OpenSession())
			{
				var posts = session.Query<TotalVotesUp.ReduceResult, TotalVotesUp>()
					.Customize(x => x.WaitForNonStaleResultsAsOfNow())
					.Single();

				Assert.Equal(10, posts.TotalVotesUp);
			}
		}

		[Fact]
		public void CanDoGroupByMapReduceOnAllShards()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				session.Store(new Post { Title = "Item 1", PublishAt = new DateTime(2012, 3, 14)});
				session.Store(new Post { Title = "Item 2", PublishAt = new DateTime(2012, 3, 14) });
				session.Store(new Post { Title = "Item 3", PublishAt = new DateTime(2012, 3, 15) });
				session.Store(new Post { Title = "Item 4", PublishAt = new DateTime(2012, 3, 15) });
				session.Store(new Post { Title = "Item 5", PublishAt = new DateTime(2012, 3, 15) });
				session.SaveChanges();
			}

			new TotalPostsPerDay().Execute(ShardedDocumentStore);
			using (var session = ShardedDocumentStore.OpenSession())
			{
				var posts = session.Query<TotalPostsPerDay.ReduceResult, TotalPostsPerDay>()
					.Customize(x => x.WaitForNonStaleResultsAsOfNow())
					.OrderBy(x => x.PublishAt)
					.ToList();

				Assert.Equal(2, posts.Count);
				Assert.Equal(2, posts[0].Count);
				Assert.Equal(3, posts[1].Count);
			}
		}

		[Fact]
		public void CanMapOnAllShards()
		{
			using (var session = ShardedDocumentStore.OpenSession())
			{
				session.Store(new Post {Title = "Item 1", Content = "Content Sample 1", VotesUpCount = 99});
				session.Store(new Post {Title = "Item 2", Content = "Content Sample 1", VotesUpCount = 100});
				session.Store(new Post {Title = "Item 3", Content = "Content Sample 1", VotesUpCount = 200});
				session.Store(new Post {Title = "Item 4", Content = "Content Sample 1", VotesUpCount = 4});
				session.Store(new Post {Title = "Item 5", Content = "Content Sample 1", VotesUpCount = 5});
				session.Store(new Post {Title = "Item 6", Content = "Content Sample 1", VotesUpCount = 9});
				session.Store(new Post {Title = "Item 7", Content = "Content Sample 1", VotesUpCount = 1});
				session.Store(new Post {Title = "Item 8", Content = "Content Sample 1", VotesUpCount = 46});
				session.Store(new Post {Title = "Item 9", Content = "Content Sample 1", VotesUpCount = 84});
				session.Store(new Post {Title = "Item 10", Content = "Content Sample 1", VotesUpCount = 14});
				session.SaveChanges();
			}

			new PostSearch().Execute(ShardedDocumentStore);
			using (var session = ShardedDocumentStore.OpenSession())
			{
				var posts = session.Query<PostSearch.Result, PostSearch>()
					.Customize(x => x.WaitForNonStaleResultsAsOfNow())
					.Where(x => x.Query == "Content Sample 1")
					.OrderByDescending(x => x.VotesUpCount)
					.Take(2)
					.As<Post>()
					.ToList();

				Assert.Equal(2, posts.Count);
				Assert.Equal("Item 3", posts[0].Title);
				Assert.Equal("Item 2", posts[1].Title);
			}
		}
	}

	public class TotalVotesUp : AbstractIndexCreationTask<Post, TotalVotesUp.ReduceResult>
	{
		public class ReduceResult
		{
			public int TotalVotesUp { get; set; }
		}

		public TotalVotesUp()
		{
			Map = posts => from post in posts
			               select new {TotalVotesUp = post.VotesUpCount};

			Reduce = results => from result in results
			                    group result by "constant"
			                    into g
			                    select new {TotalVotesUp = g.Sum(x => x.TotalVotesUp)};
		}
	}

	public class TotalPostsPerDay : AbstractIndexCreationTask<Post, TotalPostsPerDay.ReduceResult>
	{
		public class ReduceResult
		{
			public DateTime PublishAt { get; set; }
			public int Count { get; set; }
		}

		public TotalPostsPerDay()
		{
			Map = posts => from post in posts
			               select new {post.PublishAt, Count = 1};

			Reduce = results => from result in results
			                    group result by result.PublishAt
			                    into g
			                    select new {Count = g.Sum(x => x.Count), PublishAt = g.Key};
		}
	}

	public class PostSearch : AbstractIndexCreationTask<Post, PostSearch.Result>
	{
		public class Result
		{
			public string Query { get; set; }
			public int VotesUpCount { get; set; }
		}

		public PostSearch()
		{
			Map = posts => from post in posts
			               select new
			                      	{
			                      		Query = new object[]
			                      		        	{
			                      		        		post.Title,
			                      		        		post.Content,
			                      		        	},
			                      		post.VotesUpCount
			                      	};

			Index(result => result.Query, FieldIndexing.Analyzed);
			Sort(result => result.VotesUpCount, SortOptions.Int);
		}
	}
}