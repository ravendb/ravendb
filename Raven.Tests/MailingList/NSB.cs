using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class NSB : RavenTest
	{
		[Fact]
		public void CanDisableRequestProcessing()
		{
			using(GetNewServer())
			using(var store = new DocumentStore
			{
				Url = "http://localhost:8079",
			}.Initialize())
			{
				store.JsonRequestFactory.DisableRequestCompression = true;
				using(var s = store.OpenSession())
				{
					s.Store(new {Ayende = "Oren", Id = "users/oren"});
					s.SaveChanges();
				}
				using (var s = store.OpenSession())
				{
					Assert.Equal("Oren", s.Load<dynamic>("users/oren").Ayende);
				}



			}
		}
	}
}