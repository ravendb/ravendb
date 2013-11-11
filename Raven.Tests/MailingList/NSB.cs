using Xunit;

namespace Raven.Tests.MailingList
{
	public class NSB : RavenTest
	{
		[Fact]
		public void CanDisableRequestProcessing()
		{
			using(var store = NewRemoteDocumentStore())
			{
				store.JsonRequestFactory.DisableRequestCompression = true;
				using(var session = store.OpenSession())
				{
					session.Store(new {Ayende = "Oren", Id = "users/oren"});
					session.SaveChanges();
				}
				using (var s = store.OpenSession())
				{
					Assert.Equal("Oren", s.Load<dynamic>("users/oren").Ayende);
				}
			}
		}
	}
}