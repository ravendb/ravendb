using Raven.Client.Document;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class IteratingTwice : RemoteClientTest
	{
		[Fact]
		public void WillResultInTheSameResults()
		{
			using(GetNewServer())
			using(var store = new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize())
			{
				using(var s = store.OpenSession())
				{
					s.Store(new User());
					s.Store(new User());
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var query = s.Query<User>();

					for (int i = 0; i < 5; i++)
					{
						foreach (var user in query)
						{
							Assert.NotNull(user.Id);
						}
					}
				}
			}
		}
	}
}