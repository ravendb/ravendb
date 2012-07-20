// -----------------------------------------------------------------------
//  <copyright file="Security.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.ComponentModel.Composition.Hosting;
using System.Net;
using System.Reactive.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Database;
using Raven.Database.Server;
using Raven.Database.Server.Security.OAuth;
using Xunit;

namespace Raven.Tests.Notifications
{
	public class SecurityOAuth : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
			configuration.AuthenticationMode = "oauth";
			configuration.Catalog.Catalogs.Add(new TypeCatalog(typeof(DummyAuthenticateClient)));

			configuration.PostInit();
		}

		public class DummyAuthenticateClient : IAuthenticateClient
		{
			public bool Authenticate(DocumentDatabase currentDatabase, string username, string password, out AccessTokenBody.DatabaseAccess[] allowedDatabases)
			{
				allowedDatabases = new[]
				{
					new AccessTokenBody.DatabaseAccess
					{
						TenantId = "*"
					},
				};
				return username == "test" && password == "pass";
			}
		}

		protected override void CreateDefaultIndexes(Client.IDocumentStore documentStore)
		{
		}

		[Fact]
		public void WithOAuth()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Credentials = new NetworkCredential("test", "pass"),
				Url = "http://localhost:8079",
			}.Initialize())
			{
				store.JsonRequestFactory.
					EnableBasicAuthenticationOverUnsecureHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers =
					true;
				var list = new BlockingCollection<DocumentChangeNotification>();
				var taskObservable = store.Changes();
				taskObservable.Task.Wait();
				var documentSubscription = taskObservable.ForDocument("items/1");
				documentSubscription.Task.Wait();
				documentSubscription
					.Subscribe(list.Add);

				using (var session = store.OpenSession())
				{
					session.Store(new ClientServer.Item(), "items/1");
					session.SaveChanges();
				}

				DocumentChangeNotification changeNotification;
				Assert.True(list.TryTake(out changeNotification, TimeSpan.FromSeconds(2)));

				Assert.Equal("items/1", changeNotification.Name);
				Assert.Equal(changeNotification.Type, DocumentChangeTypes.Put);
			}
		}
	}
}