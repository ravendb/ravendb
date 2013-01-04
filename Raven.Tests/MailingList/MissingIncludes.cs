using Raven.Client;
using Raven.Client.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class MissingIncludes : RavenTest
	{
		public class Item
		{
			public string Parent;
		}

		[Fact]
		public void WontGenerateRequestOnMissing_Load()
		{
			using(var store = NewRemoteDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Parent = "items/2"
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Include<Item>(x => x.Parent).Load(1);
					Assert.Null(session.Load<Item>(2));
					Assert.Equal(1, session.Advanced.NumberOfRequests);
				}
			}
		}

		[Fact]
		public void WontGenerateRequestOnMissing_Query()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Parent = "items/2"
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Query<Item>().Include(x => x.Parent).Customize(x=>x.WaitForNonStaleResults()).ToList();
					Assert.Null(session.Load<Item>(2));
					Assert.Equal(1, session.Advanced.NumberOfRequests);
				}
			}
		}
	}
}