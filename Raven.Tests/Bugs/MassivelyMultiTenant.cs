using System;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Database.Extensions;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class MassivelyMultiTenant : RemoteClientTest, IDisposable
	{
		private readonly string path;
		private readonly RavenDbServer ravenDbServer;

		protected override void ModifyConfiguration(Database.Config.RavenConfiguration ravenConfiguration)
		{
			ravenConfiguration.DefaultStorageTypeName = "esent";
		}

		public MassivelyMultiTenant()
		{
			path = GetPath("MassivelyMultiTenant");
			ravenDbServer = GetNewServer(dataDirectory: path);
		}

		[Fact]
		public void CanHaveLotsOf_ACTIVE_Tenants()
		{
			for (int i = 0; i < 20; i++)
			{
				var databaseName = "Tenants" + i;
				using (var documentStore = new DocumentStore { Url = "http://localhost:8079", DefaultDatabase = databaseName }.Initialize())
				{
					documentStore.DatabaseCommands.EnsureDatabaseExists(databaseName);
				}
			}
		}

		public override void Dispose()
		{
			ravenDbServer.Dispose();
			IOExtensions.DeleteDirectory(path);
			base.Dispose();
		}
	}
}