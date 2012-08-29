using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Indexing
{
	public class DynamicQueriesCanSort : RavenTest
	{
		[Fact]
		public void CanSortOnDynamicIndexOnFieldWhichWeDontQuery()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					for (int i = 0; i < 5; i++)
					{
						session.Store(new {Value = i});
					}
					session.SaveChanges();
				}
	
				using (var session = store.OpenSession())
				{
					var array = session.Advanced.LuceneQuery<dynamic>()
						.AddOrder("Value", true)
						.ToArray();

					Assert.Equal(4, array[0].Value);
					Assert.Equal(3, array[1].Value);
					Assert.Equal(2, array[2].Value);
					Assert.Equal(1, array[3].Value);
					Assert.Equal(0, array[4].Value);
				}
			}
		}
	}
}