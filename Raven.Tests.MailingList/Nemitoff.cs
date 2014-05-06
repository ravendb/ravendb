using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Nemitoff : RavenTest
	{
		 [Fact]
		 public void LoadAfterEvict()
		 {
			 using(var store = NewDocumentStore())
			 {
				 using(var session = store.OpenSession())
				 {
					 session.Store(new Item());
					 session.SaveChanges();
				 }

				 using(var session = store.OpenSession())
				 {
					 var x = session.Load<Item>(1);
					 Assert.NotNull(x);
					 session.Advanced.Evict(x);
					 x = session.Load<Item>(1);
					 Assert.NotNull(x);

				 }
			 }
		 }

		 public class Item{}
	}
}