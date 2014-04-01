using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Build570 : RavenTest
	{
		[Fact]
		public void DoesNotFail()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item {Name = "a"});
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