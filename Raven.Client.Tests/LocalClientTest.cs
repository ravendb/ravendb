using System;
using System.IO;
using System.Reflection;
using Raven.Client.Document;
using Raven.Client.Tests.Document;
using Raven.Database;
using Raven.Database.Extensions;

namespace Raven.Client.Tests
{
	public abstract class LocalClientTest
	{
		private string path;

		protected DocumentStore NewDocumentStore()
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DocumentStoreServerTests)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);

            IOExtensions.DeleteDirectory(path);

            var documentStore = new DocumentStore
			{
				Configuration = new RavenConfiguration
				{
					DataDirectory = path,
					RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
				}

			};
			documentStore.Initialize();
			return documentStore;
		}
	}
}