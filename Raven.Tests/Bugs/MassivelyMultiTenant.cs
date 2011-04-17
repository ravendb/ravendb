using Raven.Client.Document;
using Raven.Client.Extensions;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class MassivelyMultiTenant : RemoteClientTest
	{
		protected override void ConfigureServer(Database.Config.RavenConfiguration ravenConfiguration)
		{
			ravenConfiguration.DefaultStorageTypeName = "esent";
		}

		[Fact]
		public void CanHaveLotsOf_ACTIVE_Tenants()
		{
			  using(GetNewServer())
			  {
				  for (int i = 0; i < 20; i++)
				  {
					  var databaseName = "Tenants" + i;
					  var documentStore = new DocumentStore { Url = "http://localhost:8080", DefaultDatabase = databaseName }.Initialize();

					  documentStore.DatabaseCommands.EnsureDatabaseExists(databaseName);
				  }
			  }
		}
	}
}