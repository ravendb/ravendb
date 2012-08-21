//-----------------------------------------------------------------------
// <copyright file="DocumentUrl.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Client.Document;
using Raven.Database.Server;
using Raven.Tests.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class DocumentUrl : RavenTest
	{
		[Fact]
		public void CanGetFullUrl()
		{
			using (var store = NewDocumentStore())
			{
				store.Configuration.Port = 8079;
				using (var server = new HttpServer(store.Configuration, store.DocumentDatabase))
				{
					server.StartListening();
					using (var documentStore = new DocumentStore
					{
						Url = "http://localhost:8079"
					}.Initialize())
					{
						var session = documentStore.OpenSession();

						var entity = new LinqIndexesFromClient.User();
						session.Store(entity);

						Assert.Equal("http://localhost:8079/docs/users/1",
						             session.Advanced.GetDocumentUrl(entity));
					}
				}
			}
		}

		[Fact]
		public void CanGetFullUrlWithSlashOnTheEnd()
		{
			using (var store = NewDocumentStore())
			{
				store.Configuration.Port = 8079;
				using (var server = new HttpServer(store.Configuration, store.DocumentDatabase))
				{
					server.StartListening();
					using (var documentStore = new DocumentStore
					{
						Url = "http://localhost:8079/"
					}.Initialize())
					{

						var session = documentStore.OpenSession();

						var entity = new LinqIndexesFromClient.User();
						session.Store(entity);

						Assert.Equal("http://localhost:8079/docs/users/1",
						             session.Advanced.GetDocumentUrl(entity));
					}
				}
			}
		}
	}
}
