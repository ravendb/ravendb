using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Voron
{
	public class StorageIntegrationTests : RavenTest
	{
		[Fact]
		public void RemoteStorageInitialized_Exception_Not_Thrown()
		{
			Assert.DoesNotThrow(() =>
			{
				using (var ravenDbServer = GetNewServer(requestedStorage: "voron", runInMemory: false))
				{
					using (NewRemoteDocumentStore(requestedStorage: "voron", runInMemory: false, ravenDbServer: ravenDbServer))
					{
					}

					Assert.Equal(ravenDbServer.Database.TransactionalStorage.FriendlyName, "Voron");
				}
			});
		}
	}
}
