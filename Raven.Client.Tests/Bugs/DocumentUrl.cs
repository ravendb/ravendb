using Raven.Client.Document;
using Raven.Client.Tests.Indexes;
using Raven.Database.Server;
using Raven.Server;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
	public class DocumentUrl : LocalClientTest
	{
		[Fact]
		public void CanGetFullUrl()
		{
			using (var store = NewDocumentStore())
			using (var server = new HttpServer(store.Configuration, store.DocumentDatabase))
			{
				server.Start();
				var documentStore = new DocumentStore
				{
					Url = "http://localhost:8080"
				}.Initialize();

				var session = documentStore.OpenSession();

				var entity = new LinqIndexesFromClient.User();
				session.Store(entity);

				Assert.Equal("http://localhost:8080/docs/users/1",
					session.GetDocumentUrl(entity));

			}
		}

		[Fact]
		public void CanGetFullUrlWithSlashOnTheEnd()
		{
			using (var store = NewDocumentStore())
			using (var server = new HttpServer(store.Configuration, store.DocumentDatabase))
			{
				server.Start();
				var documentStore = new DocumentStore
				{
					Url = "http://localhost:8080/"
				}.Initialize();

				var session = documentStore.OpenSession();

				var entity = new LinqIndexesFromClient.User();
				session.Store(entity);

				Assert.Equal("http://localhost:8080/docs/users/1",
					session.GetDocumentUrl(entity));

			}
		}
	}
}