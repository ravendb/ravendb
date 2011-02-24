using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class VeryBigResultSet : LocalClientTest
	{
		[Fact]
		public void CanGetVeryBigResultSetsEvenThoughItIsBadForYou()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 15000; i++)
					{
						session.Store(new User { });
					}
					session.SaveChanges();
				}

				store.Configuration.MaxPageSize = 20000;

				using (var session = store.OpenSession())
				{
					var users = session.Query<User>().Take(20000).ToArray();
					Assert.Equal(15000, users.Length);
				}
			}
		}
	}
}