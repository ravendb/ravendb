using Xunit;
using System.Linq;

namespace Raven.Client.Tests.Bugs
{
	public class ReuseQuery : BaseClientTest
	{
		[Fact]
		public void CanReuseQuery()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					var query = session.Query<object>(RavenExtensions.Raven_DocumentByEntityName);

					query.Count();
					query.ToList();
				}
			}
		}
	}
}