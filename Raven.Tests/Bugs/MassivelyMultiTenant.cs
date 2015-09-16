using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Bugs
{
	public class MassivelyMultiTenant : RavenTest
	{
        [Theory]
        [PropertyData("Storages")]
		public void CanHaveLotsOf_ACTIVE_Tenants(string storage)
		{
            using (GetNewServer(requestedStorage: storage))
            {
                for (int i = 0; i < 20; i++)
                {
                    var databaseName = "Tenants" + i;
                    using (var documentStore = new DocumentStore { Url = "http://localhost:8079", DefaultDatabase = databaseName }.Initialize())
                    {
                        documentStore.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists(databaseName);
                    }
                }
            }
		}
	}
}
