using Xunit;
using System.Linq;

namespace Raven.Client.Tests.Bugs
{
	public class ReuseQuery : LocalClientTest
	{
		[Fact]
		public void CanReuseQuery()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					var query = session.Query<object>(RavenExtensions.RavenDocumentByEntityName);

					query.Count();
					query.ToList();
				}
			}
		}
	}
}