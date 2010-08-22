using System;
using System.IO;
using System.Reflection;
using Raven.Client.Document;
using Raven.Client.Tests.Document;
using Raven.Database;

namespace Raven.Client.Tests
{
	public abstract class LocalClientTest
	{
		private string path;

		protected DocumentStore NewDocumentStore()
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DocumentStoreServerTests)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);

            if (Directory.Exists(path))
                Directory.Delete(path, true);
            
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