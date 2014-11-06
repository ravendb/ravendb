using System;
using System.Collections.Specialized;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Storage.Esent;

namespace Raven.Tests.FileSystem
{
	public class StorageTest : IDisposable
	{
		protected readonly TransactionalStorage transactionalStorage;

		public StorageTest()
		{
			var configuration = new InMemoryRavenConfiguration
			                    {
				                    Settings = new NameValueCollection(), 
									FileSystem =
									{
										DataDirectory = "test"
									}
			                    };

			IOExtensions.DeleteDirectory("test");
			transactionalStorage = new TransactionalStorage(configuration);
			transactionalStorage.Initialize();
		}

		public void Dispose()
		{
			transactionalStorage.Dispose();
		}
	}
}