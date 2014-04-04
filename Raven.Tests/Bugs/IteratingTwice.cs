using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class IteratingTwice : RavenTest
	{
		[Fact]
		public void WillResultInTheSameResults()
		{
			using(GetNewServer())
			using(var store = new DocumentStore
			{
				Url = "http://localhost:8079"
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

		[Fact]
		public void WillResultInTheSameResults_Lucene()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User());
					s.Store(new User());
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
                    var query = s.Advanced.DocumentQuery<User>();

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

		[Fact]
		public async Task WillResultInTheSameResults_Lucene_Async()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User());
					s.Store(new User());
					s.SaveChanges();
				}

				using (var s = store.OpenAsyncSession())
				{
                    var query = s.Advanced.AsyncDocumentQuery<User>();

					for (int i = 0; i < 5; i++)
					{
						var list = await query.ToListAsync();
						foreach (var user in list)
						{
							Assert.NotNull(user.Id);
						}
					}
				}
			}
		}
	}
}
