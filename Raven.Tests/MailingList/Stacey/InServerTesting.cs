using Raven.Client;
using Raven.Client.Document;
using Xunit;
using Raven.Client.Linq;
using Enumerable = System.Linq.Enumerable;

namespace Raven.Tests.MailingList.Stacey
{
	public class InServerTesting : RavenTest
	{
		[Fact]
		public void ngram_search_not_empty()
		{
			using(GetNewServer())
			using (var database = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				new ImageByName().Execute(database);

				using (var session = database.OpenSession())
				{
					session.Store(new Image { Id = "1", Name = "Great Photo buddy" });
					session.Store(new Image { Id = "2", Name = "Nice Photo of the sky" });
					session.SaveChanges();
				}
				using (var session = database.OpenSession())
				{
					var images = Enumerable.ToList<Image>(session.Query<Image, ImageByName>()
						               	.Customize(x => x.WaitForNonStaleResults())
						               	.OrderBy(x => x.Name)
						               	.Search(x => x.Name, "phot"));

					Assert.NotEmpty(images);
				}
			}
		}
	}
}