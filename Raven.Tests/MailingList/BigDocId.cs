using Xunit;

namespace Raven.Tests.MailingList
{
	public class BigDocId : RavenTest
	{
		[Fact]
		public void CanCreateBigId()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					session.Store(new User
					{
						Id = new string('*', 512)
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					Assert.NotNull(session.Load<User>(new string('*', 512)));
				}
			}
		}
	}
}