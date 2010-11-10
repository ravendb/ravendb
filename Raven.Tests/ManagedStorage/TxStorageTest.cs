using System.IO;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Plugins;
using Raven.Database.Storage;
using Raven.Storage.Managed;

namespace Raven.Storage.Tests
{
	public class TxStorageTest
	{
		public TxStorageTest()
		{
			if(Directory.Exists("test"))
				Directory.Delete("test", true);
		}

        public ITransactionalStorage NewTransactionalStorage()
        {
            var newTransactionalStorage = new TransactionalStorage(new RavenConfiguration
            {
                DataDirectory = "test",
            }, () => { })
            {
                DocumentCodecs = new AbstractDocumentCodec[0]
            };
            newTransactionalStorage.Initialize(new DummyUuidGenerator());
            return newTransactionalStorage;
        }
	}
}