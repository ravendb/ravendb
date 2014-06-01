using System;
using System.Collections.Specialized;

using Raven.Database.Config;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Storage.Esent;

namespace RavenFS.Tests
{
	public class StorageTest : IDisposable
	{
		protected readonly TransactionalStorage transactionalStorage;

		public StorageTest()
		{
			var configuration = new InMemoryRavenConfiguration
			{
				FileSystemDataDirectory = "test",
				Settings = new NameValueCollection()
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