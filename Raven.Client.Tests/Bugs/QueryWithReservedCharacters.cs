using Raven.Client.Client;
using Xunit;
using System.Linq;

namespace Raven.Client.Tests.Bugs
{
	public class QueryWithReservedCharacters : BaseClientTest
	{
		[Fact]
		public void CanQueryWithReservedCharactersWithoutException()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.LuceneQuery<object>("Raven/DocumentsByEntityName")
						.Where(RavenQuery.Escape("foo]]]]"))
						.ToList();
				}
			}
		}
	}
}