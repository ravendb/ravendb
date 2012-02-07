using System.Linq;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Alexander : RavenTest
	{
		[Fact]
		public void QueryById()
		{
			using (GetNewServer())
			using (var documentStore = new DocumentStore()
			{
				Url = "http://localhost:8079",
				Conventions = {DefaultQueryingConsistency = ConsistencyOptions.QueryYourWrites}
			}.Initialize())
			{

				var documentSession = documentStore.OpenSession();

				var casino = new Casino("Cities/123456", "address", "name");
				documentSession.Store(casino);
				documentSession.SaveChanges();

				var casinoFromDb = documentSession.Query<Casino>().Where(x => x.Id == casino.Id).Single();

				Assert.NotNull(casinoFromDb);

			}
		}

		public class Casino
		{
			public string Id { get; set; }
			public string CityId { get; set; }
			public string Address { get; set; }
			public string Title { get; set; }

			public Casino()
			{
				
			}

			public Casino(string cityId, string address, string name)
			{
				CityId = cityId;
				Address = address;
				Title = name;
			}
		}
	}
}