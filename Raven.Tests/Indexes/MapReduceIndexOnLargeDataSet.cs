using System;
using Raven.Abstractions.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Indexes
{
	public class MapReduceIndexOnLargeDataSet : RavenTest
	{
		[Fact]
		public void WillNotProduceAnyErrors()
		{
			using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from x in docs select new { x.Name, Count = 1}",
					Reduce = "from r in results group r by r.Name into g select new { Name = g.Key, Count = g.Sum(x=>x.Count) }"
				});

				for (int i = 0; i < 200; i++)
				{
					using (var s = store.OpenSession())
					{
						for (int j = 0; j < 25; j++)
						{
							s.Store(new {Name = "User #" + j});
						}
						s.SaveChanges();
					}
				}

				using (var s = store.OpenSession())
				{
					var ret = s.Query<dynamic>("test")
						.Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(1)))
						.ToArray();
					Assert.Equal(25, ret.Length);
					foreach (var x in ret)
						Assert.Equal("200", x.Count);
				}

				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
			}
		}
	}
}