using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class QueryingOnValueWithMinus : RavenTest
	{
		[Fact]
		public void CanQueryOnValuesContainingMinus()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new{Name = "Bruce-Lee"});
					session.SaveChanges();
				}

				using(var session = store.OpenSession())
				{
					var list = session.Advanced.LuceneQuery<dynamic>()
						.WhereEquals("Name","Bruce-Lee")
						.ToList();

					Assert.Equal(1, list.Count);
				}
			}
		}
	}
}
