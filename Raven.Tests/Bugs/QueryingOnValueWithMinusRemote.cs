using System.Linq;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class QueryingOnValueWithMinusRemote : RavenTest
	{
		[Fact]
		public void CanQueryOnValuesContainingMinus()
		{
			using(GetNewServer())
			using (var store = new DocumentStore{Url = "http://localhost:8079"}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new { Name = "Bruce-Lee" });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
                    var list = session.Advanced.DocumentQuery<object>()
						.WhereEquals("Name", "Bruce-Lee")
						.ToList<object>();

					Assert.Equal(1, list.Count);
				}
			}
		}
	}
}