// -----------------------------------------------------------------------
//  <copyright file="Security.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using Lucene.Net.Util;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Server;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Notifications
{
	public class SecurityOAuth : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
			configuration.PostInit();
		}


		protected override void CreateDefaultIndexes(Client.IDocumentStore documentStore)
		{
		}

		[Fact]
		public void WithOAuthOnSystemDatabase()
		{
			using (var server = GetNewServer())
			{
				server.Database.Put("Raven/ApiKeys/test", null, RavenJObject.FromObject(new ApiKeyDefinition
				{
					Name = "test",
					Secret = "test",
					Enabled = true,
					Databases = new List<DatabaseAccess>
					{
						new DatabaseAccess {TenantId = "<system>"},
					}
				}), new RavenJObject(), null);

				using (var store = new DocumentStore
				{
					ApiKey = "test/test",
					Url = "http://localhost:8079",
					Conventions = { FailoverBehavior = FailoverBehavior.FailImmediately }
				}.Initialize())
				{
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

					Assert.Equal("items/1", changeNotification.Id);
					Assert.Equal(changeNotification.Type, DocumentChangeTypes.Put);
				}
			}
		}

		[Fact]
		public void WithOAuthWrongKeyFails()
		{
			using (var server = GetNewServer())
			{
				server.Database.Put("Raven/ApiKeys/test", null, RavenJObject.FromObject(new ApiKeyDefinition
				{
					Name = "test",
					Secret = "test",
					Enabled = true,
					Databases = new List<DatabaseAccess>
					{
						new DatabaseAccess {TenantId = "*"},
					}
				}), new RavenJObject(), null);

				using (var store = new DocumentStore
				{
					ApiKey = "NotRealKeys",
					Url = "http://localhost:8079",
					Conventions = { FailoverBehavior = FailoverBehavior.FailImmediately }
				}.Initialize())
				{
					Assert.Throws<InvalidOperationException>(() =>
					{
						using (var session = store.OpenSession())
						{
							session.Store(new ClientServer.Item(), "items/1");
							session.SaveChanges();
						}
					});
				}
			}
		}

		[Fact]
		public void WithOAuthOnSpecificDatabase()
		{
			using (var server = GetNewServer())
			{
				server.Database.Put("Raven/Databases/OAuthTest", null, RavenJObject.FromObject(new DatabaseDocument
				{
					Disabled = false,
					Id = "Raven/Databases/OAuthTest",
					Settings = new IdentityDictionary<string, string>
					{
						{"Raven/DataDir", "~\\Databases\\OAuthTest"}
					}
				}), new RavenJObject(), null);

				server.Database.Put("Raven/ApiKeys/test", null, RavenJObject.FromObject(new ApiKeyDefinition
				{
					Name = "test",
					Secret = "test",
					Enabled = true,
					Databases = new List<DatabaseAccess>
					{
						new DatabaseAccess {TenantId = "OAuthTest"},
					}
				}), new RavenJObject(), null);

				using (var store = new DocumentStore
				{
					ApiKey = "test/test",
					DefaultDatabase = "OAuthTest",
					Url = "http://localhost:8079",
					Conventions = { FailoverBehavior = FailoverBehavior.FailImmediately }
				}.Initialize())
				{
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

					Assert.Equal("items/1", changeNotification.Id);
					Assert.Equal(changeNotification.Type, DocumentChangeTypes.Put);
				}
			}
		}

		[Fact]
		public void WithOAuthOnSpecificDatabaseWontWorkForAnother()
		{
			using (var server = GetNewServer())
			{
				server.Database.Put("Raven/Databases/OAuthTest1", null, RavenJObject.FromObject(new DatabaseDocument
				{
					Disabled = false,
					Id = "Raven/Databases/OAuthTest1",
					Settings = new IdentityDictionary<string, string>
					{
						{"Raven/DataDir", "~\\Databases\\OAuthTest1"}
					}
				}), new RavenJObject(), null);

				server.Database.Put("Raven/Databases/OAuthTest2", null, RavenJObject.FromObject(new DatabaseDocument
				{
					Disabled = false,
					Id = "Raven/Databases/OAuthTest2",
					Settings = new IdentityDictionary<string, string>
					{
						{"Raven/DataDir", "~\\Databases\\OAuthTest2"}
					}
				}), new RavenJObject(), null);

				server.Database.Put("Raven/ApiKeys/test", null, RavenJObject.FromObject(new ApiKeyDefinition
				{
					Name = "test",
					Secret = "test",
					Enabled = true,
					Databases = new List<DatabaseAccess>
					{
						new DatabaseAccess {TenantId = "OAuthTest1"},
					}
				}), new RavenJObject(), null);

				using (var store = new DocumentStore
				{
					ApiKey = "test/test",
					DefaultDatabase = "OAuthTest2",
					Url = "http://localhost:8079",
					Conventions = { FailoverBehavior = FailoverBehavior.FailImmediately }
				}.Initialize())
				{
					Assert.Throws<WebException>(() =>
					{
						using (var session = store.OpenSession())
						{
							session.Store(new ClientServer.Item(), "items/1");
							session.SaveChanges();
						}
					});
				}
			}
		}

		[Fact]
		public void WithOAuthWithStarWorksForAnyDatabaseOtherThenSystem()
		{
			using (var server = GetNewServer())
			{
				server.Database.Put("Raven/Databases/OAuthTest", null, RavenJObject.FromObject(new DatabaseDocument
				{
					Disabled = false,
					Id = "Raven/Databases/OAuthTest",
					Settings = new IdentityDictionary<string, string>
					{
						{"Raven/DataDir", "~\\Databases\\OAuthTest"}
					}
				}), new RavenJObject(), null);

				server.Database.Put("Raven/ApiKeys/test", null, RavenJObject.FromObject(new ApiKeyDefinition
				{
					Name = "test",
					Secret = "test",
					Enabled = true,
					Databases = new List<DatabaseAccess>
					{
						new DatabaseAccess {TenantId = "*"},
					}
				}), new RavenJObject(), null);

				using (var store = new DocumentStore
				{
					ApiKey = "test/test",
					DefaultDatabase = "OAuthTest",
					Url = "http://localhost:8079",
					Conventions = { FailoverBehavior = FailoverBehavior.FailImmediately }
				}.Initialize())
				{
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

					Assert.Equal("items/1", changeNotification.Id);
					Assert.Equal(changeNotification.Type, DocumentChangeTypes.Put);
				}

				using (var store = new DocumentStore
				{
					ApiKey = "test/test",
					Url = "http://localhost:8079",
					Conventions = { FailoverBehavior = FailoverBehavior.FailImmediately }
				}.Initialize())
				{
					Assert.Throws<WebException>(() =>
					{
						using (var session = store.OpenSession())
						{
							session.Store(new ClientServer.Item(), "items/1");
							session.SaveChanges();
						}
					});
				}
			}
		}
	}
}