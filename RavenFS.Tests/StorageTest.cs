using System;
using System.Collections.Specialized;
using Raven.Database.Server.RavenFS.Extensions;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Storage.Esent;
using Raven.Json.Linq;

namespace RavenFS.Tests
{
	public class StorageTest : IDisposable
	{
		protected readonly TransactionalStorage transactionalStorage;

		public StorageTest()
		{
			IOExtensions.DeleteDirectory("test");
			transactionalStorage = new TransactionalStorage("test", new RavenJObject());
			transactionalStorage.Initialize();
		}

		public void Dispose()
		{
			transactionalStorage.Dispose();
		}
	}
}