using Raven.Client.Document;
using Raven.Tests.Bugs;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class MultiLoad : RavenTest
	{
		[Fact]
		public void WillNotReturnDistinctResults_Embedded()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					for (int i = 0; i < 5; i++)
					{
						s.Store(new Item
						{
							Version = i.ToString()
						});
					}
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var items = s.Load<Item>("items/1", "items/2", "items/1");
					Assert.Equal(3, items.Length);
				}
			}
		}

		[Fact]
		public void WillNotReturnDistinctResults_Remote()
		{
			using(GetNewServer())
			using (var store = new DocumentStore{Url = "http://localhost:8079"}.Initialize())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 5; i++)
					{
						s.Store(new Item
						{
							Version = i.ToString()
						});
					}
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var items = s.Load<Item>("items/1", "items/2", "items/1");
					Assert.Equal(3, items.Length);
				}
			}
		}
	}
}