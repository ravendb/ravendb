// -----------------------------------------------------------------------
//  <copyright file="BulkInsertAuth.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Issues
{
	public class BulkInsertAuth : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
			Authentication.EnableOnce();
		}

		[Fact]
		public void CanBulkInsertWithWindowsAuth()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var op = new RemoteBulkInsertOperation(new BulkInsertOptions(),
															  (ServerClient)store.DatabaseCommands, store.Changes()))
				{
					op.Write("items/1", new RavenJObject(), new RavenJObject());
				}
				Assert.NotNull(store.DatabaseCommands.Get("items/1"));
			}
		}
	}

	public class BulkInsertOAuth : RavenTest
	{
		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
			Authentication.EnableOnce();
		}

		protected override void ModifyServer(Server.RavenDbServer ravenDbServer)
		{
			var id = "Raven/ApiKeys/test";
			ravenDbServer.SystemDatabase.Put(id, null,
									   RavenJObject.FromObject(new ApiKeyDefinition
									   {
										   Id = id,
										   Name = "test",
										   Secret = "test",
										   Enabled = true,
										   Databases =
				                           {
					                           new DatabaseAccess {Admin = true, TenantId = "*"},
					                           new DatabaseAccess {Admin = true, TenantId = "<system>"}
				                           }
									   }), new RavenJObject(), null);
		}

		protected override void ModifyStore(DocumentStore documentStore)
		{
			documentStore.ApiKey = "test/test";
			documentStore.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
		}

		[Fact]
		public void CanBulkInsertWithApiKey()
		{
			using (var store = NewRemoteDocumentStore(enableAuthentication: true))
			{
				using (var op = new RemoteBulkInsertOperation(new BulkInsertOptions(),
															  (ServerClient)store.DatabaseCommands, store.Changes()))
				{
					op.Write("items/1", new RavenJObject(), new RavenJObject());
				}
				Assert.NotNull(store.DatabaseCommands.Get("items/1"));
			}
		}
	}
}