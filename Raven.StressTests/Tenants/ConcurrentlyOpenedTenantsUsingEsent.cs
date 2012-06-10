// -----------------------------------------------------------------------
//  <copyright file="RavenDB50.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Tests;
using Xunit;

namespace Raven.StressTests.Tenants
{
	public class ConcurrentlyOpenedTenantsUsingEsent : RavenTest
	{
		protected override void ModifyConfiguration(RavenConfiguration ravenConfiguration)
		{
			ravenConfiguration.RunInMemory = false;
			ravenConfiguration.DefaultStorageTypeName = "esent";
		}

		[Fact]
		public void CanConcurrentlyPutDocsToDifferentTenants()
		{
			const int count = 100;
			using (GetNewServer())
			using (var documentStore = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				for (int i = 1; i <= count; i++)
				{
					var tenantName = "Tenant-" + i;
					documentStore.DatabaseCommands.EnsureDatabaseExists(tenantName);
				}

				for (int j = 0; j < 50; j++)
				{
					for (int i = 1; i <= count; i++)
					{
						var tenantName = "Tenant-" + i;
						var databaseCommands = documentStore.DatabaseCommands.ForDatabase(tenantName);
						databaseCommands.Put("posts/", null, new RavenJObject(), new RavenJObject());
					}
				}
			}
		}
	}
}