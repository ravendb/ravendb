//-----------------------------------------------------------------------
// <copyright file="DocumentUrl.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Client.Document;
using Raven.Database.Server;
using Raven.Tests.Indexes;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Bugs
{
	public class DocumentUrl : RavenTest
	{
		[Theory]
		[InlineData("http://localhost:8079")]
		[InlineData("http://localhost:8079/")]
		public void CanGetFullUrl_WithSlashOnTheEnd(string url)
		{
			using (var store = NewDocumentStore())
			{
				using (var server = new HttpServer(store.Configuration, store.DocumentDatabase))
				{
					server.StartListening();
					using (var documentStore = new DocumentStore {Url = url}.Initialize())
					{
						var session = documentStore.OpenSession();

						var entity = new LinqIndexesFromClient.User();
						session.Store(entity);

						Assert.Equal("http://localhost:8079/docs/users/1", session.Advanced.GetDocumentUrl(entity));
					}
				}
			}
		}
	}
}