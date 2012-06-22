using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class NullableBoolQuery : RavenTest
	{
		public class Item
		{
			public bool? Active { get; set; }
		}

		[Fact]
		public void CanQuery_Simple()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new Item{Active = true});
					session.SaveChanges();
				}

				using(var session = store.OpenSession())
				{
					Assert.NotEmpty	(session.Query<Item>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow())
						.Where(x => x.Active == true)
						.ToList());
				}
			}
		}

		[Fact]
		public void CanQuery_Null()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item { Active = true });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var collection = session.Query<Item>()
						.Customize(x => x.WaitForNonStaleResultsAsOfNow())
						.Where(x => x.Active != null && x.Active.Value)
						.ToList();
					Assert.NotEmpty(collection);
				}
			}
		}
	}
}