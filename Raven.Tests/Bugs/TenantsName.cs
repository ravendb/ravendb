using System;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Database.Config;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class TenantsName : RavenTest
	{
		protected override void ModifyConfiguration(InMemoryRavenConfiguration ravenConfiguration)
		{
			ravenConfiguration.RunInMemory = false;
			ravenConfiguration.DefaultStorageTypeName = "esent";
		}

		[Fact]
		public void CannotContainSpaces()
		{
			using (GetNewServer())
			using (var documentStore = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				const string tenantName = "   Tenant with some    spaces     in it ";
				var exception = Assert.Throws<InvalidOperationException>(() => documentStore.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists(tenantName));
				Assert.Equal("Database name can only contain only A-Z, a-z, \"_\", \".\" or \"-\" but was: " + tenantName, exception.Message);

				var databaseCommands = documentStore.DatabaseCommands.ForDatabase(tenantName);
				exception = Assert.Throws<InvalidOperationException>(() => databaseCommands.Put("posts/", null, new RavenJObject(), new RavenJObject()));
				Assert.Equal("Could not find a database named: %20%20%20Tenant%20with%20some%20%20%20%20spaces%20%20%20%20%20in%20it%20", exception.Message);
			}
		}
	}
}