using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Bugs;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class RangeQueriesOverSum : RavenTest
	{
		public class TheIndex : AbstractIndexCreationTask<Item, TheIndex.ReduceResult>
		{
			public class ReduceResult
			{
				public string Version { get; set; }
				public int ItemsCount { get; set; }
			}

			public TheIndex()
			{
				Map = items => from item in items
				               select new
				               {
								   item.Version,
								   ItemsCount = 1
				               };
				Reduce = results =>
				         from result in results
				         group result by result.Version
				         into g
				         select new
				         {
							 Version = g.Key,
							 ItemsCount = g.Sum(x=>x.ItemsCount)
				         };
			}
		}

		[Fact]
		public void CanQueryByRangeOverMapReduce()
		{
			using(var documentStore = NewDocumentStore())
			{
				using(var session = documentStore.OpenSession())
				{
					for (int i = 0; i < 5; i++)
					{
						session.Store(new Item
						{
							Version = "a"
						});
					}
					session.SaveChanges();
				}

				new TheIndex().Execute(documentStore);

				using (var session = documentStore.OpenSession())
				{
					var results2 = session.Query<TheIndex.ReduceResult, TheIndex>()
						.Customize(x=>x.WaitForNonStaleResults())
						.Where(x => x.ItemsCount > 0)
						.ToArray();
					Assert.NotEmpty(results2);
				}
			}
		}
	}
}