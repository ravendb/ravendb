// -----------------------------------------------------------------------
//  <copyright file="ApiKeysWithMultiTenancy.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;
using Raven.Client.Extensions;

namespace Raven.Tests.Bundles.Replication.Bugs
{
	public class ApiKeysWithMultiTenancy : ReplicationBase
	{
		private const string apikey = "test/ThisIsMySecret";

		protected override void ModifyConfiguration(InMemoryRavenConfiguration serverConfiguration)
		{
			serverConfiguration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
            Authentication.EnableOnce();
		}

        protected override void ConfigureDatabase(Database.DocumentDatabase database, string databaseName = null)
		{
			database.Documents.Put("Raven/ApiKeys/test", null, RavenJObject.FromObject(new ApiKeyDefinition
			{
				Name = "test",
				Secret = "ThisIsMySecret",
				Enabled = true,
				Databases = new List<DatabaseAccess>
				{
					new DatabaseAccess {TenantId = "*", Admin = true},
					new DatabaseAccess {TenantId = Constants.SystemDatabase, Admin = true},
                    new DatabaseAccess {TenantId = databaseName, Admin = true}
				}
			}), new RavenJObject(), null);
		}

		[Fact]
		public void CanReplicationToChildDbsUsingApiKeys()
		{
			var store1 = CreateStore(configureStore: store =>
			{
				store.ApiKey = apikey;
				store.Conventions.FailoverBehavior=FailoverBehavior.FailImmediately;
			},enableAuthorization: true);
			var store2 = CreateStore(configureStore: store =>
			{
				store.ApiKey = apikey;
				store.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
			}, enableAuthorization: true);

			store1.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
			{
				Id = "repl",
				Settings =
				{
					{"Raven/RunInMemory", "true"},
					{"Raven/DataDir", "~/Databases/db1"},
					{"Raven/ActiveBundles", "Replication"}
				}
			});
			store2.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
			{
				Id = "repl",
				Settings =
				{
					{"Raven/RunInMemory", "true"},
					{"Raven/DataDir", "~/Databases/db2"},
					{"Raven/ActiveBundles", "Replication"}
				}
			});

			RunReplication(store1, store2, apiKey: apikey, db: "repl");

			using (var s = store1.OpenSession("repl"))
			{
				s.Store(new User());
				s.SaveChanges();
			}

			WaitForReplication(store2, "users/1", db: "repl");
		}
	}
}