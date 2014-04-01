// -----------------------------------------------------------------------
//  <copyright file="ReplicationWithOAuthMultipleKeys.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.SlowTests.Security.OAuth
{
	public class ReplicationWithOAuthMultipleKeys : ReplicationBase
	{
		private string[] apiKeys = new string[]
		{
			"test1/ThisIsMySecret",
			"test2/ThisIsMySecret",
		};

		private int storeCounter, databaseCounter;

		protected override void ModifyStore(DocumentStore store)
		{
			store.Conventions.FailoverBehavior = FailoverBehavior.AllowReadsFromSecondaries;
			store.Credentials = null;
			store.ApiKey = apiKeys[storeCounter++];
		}

        protected override void ConfigureDatabase(Database.DocumentDatabase database, string databaseName = null)
		{
			var apiKey = apiKeys[databaseCounter++];
			database.Documents.Put("Raven/ApiKeys/" + apiKey.Split('/')[0], null, RavenJObject.FromObject(new ApiKeyDefinition
			{
				Name = apiKey.Split('/')[0],
				Secret = apiKey.Split('/')[1],
				Enabled = true,
				Databases = new List<DatabaseAccess>
				{
					new DatabaseAccess {TenantId = "*"},
					new DatabaseAccess {TenantId = Constants.SystemDatabase},
                    new DatabaseAccess {TenantId = databaseName}
				}
			}), new RavenJObject(), null);
		}

		[Fact]
		public void Can_Failover_With_Different_Api_Key()
		{
			var store1 = CreateStore(enableAuthorization: true);
			Authentication.EnableOnce();
			var store2 = CreateStore(anonymousUserAccessMode: AnonymousUserAccessMode.None, enableAuthorization: true);

			TellFirstInstanceToReplicateToSecondInstance(apiKeys[1]);

			using (var session = store1.OpenSession())
			{
				session.Store(new Company { Name = "Hibernating Rhinos" });
				session.SaveChanges();
			}

			var company = WaitForDocument<Company>(store2, "companies/1");
			Assert.Equal("Hibernating Rhinos", company.Name);

			var serverClient = ((ServerClient)store1.DatabaseCommands);
			serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);

			servers[0].Dispose();

			using (var session = store1.OpenSession())
			{
				Assert.NotNull(session.Load<Company>(1));
			}
		}

		[Fact]
		public async Task Can_Failover_With_Different_Api_Key_async()
		{
			var store1 = CreateStore(enableAuthorization: true);
			Authentication.EnableOnce();
			var store2 = CreateStore(anonymousUserAccessMode: AnonymousUserAccessMode.None, enableAuthorization: true);

			TellFirstInstanceToReplicateToSecondInstance(apiKeys[1]);

			using (var session = store1.OpenAsyncSession())
			{
				await session.StoreAsync(new Company { Name = "Hibernating Rhinos" });
				await session.SaveChangesAsync();
			}

			var company = WaitForDocument<Company>(store2, "companies/1");
			Assert.Equal("Hibernating Rhinos", company.Name);

			var serverClient = (ServerClient)store1.DatabaseCommands;
			serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);

			servers[0].Dispose();

			using (var session = store1.OpenAsyncSession())
			{
				Assert.NotNull(await session.LoadAsync<Company>(1));
			}
		}

		[Fact]
		public void Can_replicate_indexes()
		{
			var store1 = CreateStore(enableAuthorization: true);
			Authentication.EnableOnce();
			var store2 = CreateStore(anonymousUserAccessMode: AnonymousUserAccessMode.None, enableAuthorization: true);

			TellFirstInstanceToReplicateToSecondInstance(apiKeys[1]);

			new MyIndex().Execute(store1);

			Assert.NotNull(store2.DatabaseCommands.GetIndex("MyIndex"));

			var serverClient = ((ServerClient)store1.DatabaseCommands);
			serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);

			servers[0].Dispose();

			Assert.NotNull(store1.DatabaseCommands.GetIndex("MyIndex"));
		}

		[Fact]
		public async Task Can_replicate_indexes_async()
		{
			var store1 = CreateStore(enableAuthorization: true);
			Authentication.EnableOnce();
			var store2 = CreateStore(anonymousUserAccessMode: AnonymousUserAccessMode.None, enableAuthorization: true);

			TellFirstInstanceToReplicateToSecondInstance(apiKeys[1]);

			await new MyIndex().ExecuteAsync(store1.AsyncDatabaseCommands, store1.Conventions);
			Assert.NotNull(await store2.AsyncDatabaseCommands.GetIndexAsync("MyIndex"));

			var serverClient = ((ServerClient)store1.DatabaseCommands);
			serverClient.ReplicationInformer.RefreshReplicationInformation(serverClient);

			servers[0].Dispose();

			Assert.NotNull(await store1.AsyncDatabaseCommands.GetIndexAsync("MyIndex"));
		}

		public class MyIndex : AbstractIndexCreationTask<Company>
		{
			public MyIndex()
			{
				Map = companies =>
				      from company in companies
				      select new {company.Name};
			}
		}

	}
}