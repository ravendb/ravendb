using Raven.Abstractions.Data;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Queries
{
	public class DynamicMapReduce : LocalClientTest
	{
		[Fact]
		public void CanDynamicallyQueryOverItemCount()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						s.Store(new { Name = i % 2 == 0 ? "Ayende" : "Rahien" });
					}
					s.SaveChanges();
				}
				using (var s = store.OpenSession())
				{
					var objects = s.Advanced.LuceneQuery<dynamic>()
						.GroupBy(AggregationOperation.Count)
						.WaitForNonStaleResults()
						.ToArray();

					Assert.Equal(1, objects.Length);
					Assert.Equal("10", objects[0].Count);
				}
			}
		}

		[Fact]
		public void CanDynamicallyQueryOverItemCountByName()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						s.Store(new { Name = i % 2 == 0 ? "Ayende" : "Rahien" });
					}
					s.SaveChanges();
				}
				using (var s = store.OpenSession())
				{
					var objects = s.Advanced.LuceneQuery<dynamic>()
						.GroupBy(AggregationOperation.Count, "Name")
						.WaitForNonStaleResults()
						.OrderBy("Name")
						.ToArray();


					Assert.Equal(2, objects.Length);
					Assert.Equal("5", objects[0].Count);
					Assert.Equal("Ayende", objects[0].Name);
					Assert.Equal("5", objects[1].Count);
					Assert.Equal("Rahien", objects[1].Name);
				}
			}
		}
	}
}