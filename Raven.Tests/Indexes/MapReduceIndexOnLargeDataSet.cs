using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Tests.Common;

using Xunit;

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
					           .Customize(x => x.WaitForNonStaleResults())
					           .ToArray();
					Assert.Equal(25, ret.Length);
					foreach (var x in ret)
					{
						try
						{
							Assert.Equal(200, x.Count);
						}
						catch (Exception)
						{
							PrintServerErrors(store.DocumentDatabase.Statistics.Errors);

							var missed = ret.Where(item => item.Count != 200)
							                .Select(item => "Name: " + item.Name + ". Count: " + item.Count)
							                .Cast<string>()
							                .ToList();
							Console.WriteLine("Missed documents: ");
							Console.WriteLine(string.Join(", ", missed));

							throw;
						}
					}
				}

				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
			}
		}
	}
}