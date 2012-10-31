using System;
using System.IO;
using System.Reflection;
using Raven.Client.Document;
using Raven.Server;

namespace Raven.Tests.Bundles.PeriodicBackups
{
	public class PeriodicBackupTests : IDisposable
	{
		private readonly string path;
		private readonly DocumentStore documentStore;
		private readonly RavenDbServer ravenDbServer;

		public string AwsAccessKey { get; set; }
		public string AwsSecretKey { get; set; }

		public PeriodicBackupTests()
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(PeriodicBackupTests)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);
			Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
			var ravenConfiguration = new Raven.Database.Config.RavenConfiguration
			{
				Port = 8079,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
				DataDirectory = path,
				Settings =
					{
						{"Raven/ActiveBundles", "PeriodicUpdates"}
					}
			};
			ravenConfiguration.PostInit();
			ravenDbServer = new RavenDbServer(ravenConfiguration);
			documentStore = new DocumentStore
			{
				Url = "http://localhost:8079"
			};
			documentStore.Initialize();
		}

		public void Dispose()
		{
			documentStore.Dispose();
			ravenDbServer.Dispose();
			Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
		}

		public void SetupAws(string AWSAccessKey, string AWSSecretKey)
		{
			ravenDbServer.Database.Configuration.Settings["Raven/AWSAccessKey"] = AwsSecretKey;
			ravenDbServer.Database.Configuration.Settings["Raven/AWSSecretKey"] = AwsSecretKey;
		}

		[FactIfAwsIsAvailable]
		public void testFoo()
		{

		}
	}
}
