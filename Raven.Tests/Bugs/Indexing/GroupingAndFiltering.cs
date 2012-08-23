using Raven.Abstractions.Data;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Indexing
{
	public class GroupingAndFiltering : RavenTest
	{
		[Fact]
		public void OnSameField()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					s.Advanced.LuceneQuery<dynamic>()
						.WhereEquals("Tags,Name", "Ayende")
						.GroupBy(AggregationOperation.Count, "Tags,Name")
						.ToArray();
				}
			}
		}

		[Fact]
		public void OnSameFieldDynamically()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Advanced.LuceneQuery<dynamic>()
						.WhereEquals("Tags,Name", "Ayende")
						.GroupBy(AggregationOperation.Count | AggregationOperation.Dynamic, "Tags,Name")
						.ToArray();
				}
			}
		}
	}
}