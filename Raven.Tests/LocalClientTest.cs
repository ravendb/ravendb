using System;
using System.IO;
using System.Reflection;
using System.Threading;
using Raven.Client.Client;
using Raven.Client.Document;
using Raven.Client.Tests.Document;
using Raven.Database;
using Raven.Database.Extensions;

namespace Raven.Client.Tests
{
	public abstract class LocalClientTest
	{
		private string path;

        public EmbeddableDocumentStore NewDocumentStore()
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(DocumentStoreServerTests)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);

            IOExtensions.DeleteDirectory(path);

            var documentStore = new EmbeddableDocumentStore()
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

        public void WaitForIndexing(EmbeddableDocumentStore store)
        {
            while (store.DocumentDatabase.Statistics.StaleIndexes.Length > 0)
            {
                Thread.Sleep(100);
            }
        }
	}
}
