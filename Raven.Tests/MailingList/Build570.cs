using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Build570
	{
		[Fact]
		public void DoesntFail()
		{
			using(var store = new EmbeddableDocumentStore
			{
				RunInMemory = true
			}.Initialize())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new Item{Name = "a"});
					session.SaveChanges();
				}
			}
		}

		public class Item
		{
			public string Name { get; set; }
		}
	}
}