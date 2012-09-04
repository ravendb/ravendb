using System;
using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class MapReduceWithDifferentFieldNamesFromTheStronglyTypedType : RavenTest
	{
		[Fact]
		public void WhenTheAnonymousTypeResultIsNotTheSameAsTheStronglyType_ShouldThrowAnException()
		{
			using (var embeddableDocumentStore = NewDocumentStore())
			{
				Assert.Throws<InvalidOperationException>(() => new Posts_ByMonthPublished_Count_ShouldFail().Execute(embeddableDocumentStore));
			}
		}

		[Fact]
		public void WhenTheAnonymousTypeResultIsTheSameAsTheStronglyType_ShouldNotThrowAnException()
		{
			using (var embeddableDocumentStore = NewDocumentStore())
			{
				Assert.DoesNotThrow(() => new Posts_ByMonthPublished_Count_ShouldPass().Execute(embeddableDocumentStore));
			}
		}

		private class Posts_ByMonthPublished_Count_ShouldFail : AbstractIndexCreationTask<CreateIndexesRemotely.Post, CreateIndexesRemotely.PostCountByMonth>
		{
			public Posts_ByMonthPublished_Count_ShouldFail()
			{
				Map = posts => from post in posts
							   select new { YearNameDoesNotMatch = post.PublishAt.Year, post.PublishAt.Month, Count = 1 };
				Reduce = results => from result in results
									group result by new { result.Year, result.Month }
										into g
										select new { g.Key.Year, g.Key.Month, Count = g.Sum(x => x.Count) };
			}
		}

		private class Posts_ByMonthPublished_Count_ShouldPass : AbstractIndexCreationTask<CreateIndexesRemotely.Post, CreateIndexesRemotely.PostCountByMonth>
		{
			public Posts_ByMonthPublished_Count_ShouldPass()
			{
				Map = posts => from post in posts
							   select new { post.PublishAt.Year, post.PublishAt.Month, Count = 1 };
				Reduce = results => from result in results
									group result by new { result.Year, result.Month }
										into g
										select new { g.Key.Year, g.Key.Month, Count = g.Sum(x => x.Count) };
			}
		}
	}
}
