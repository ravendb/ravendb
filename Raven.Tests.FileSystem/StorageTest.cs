using System;
using System.Collections.Specialized;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Infrastructure;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.FileSystem.Storage.Esent;
using Raven.Database.Plugins;

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
			transactionalStorage.Initialize(new UuidGenerator(), new OrderedPartCollection<AbstractFileCodec>());
		}

		public void Dispose()
		{
			transactionalStorage.Dispose();

			IOExtensions.DeleteDirectory("test");
		}
	}
}