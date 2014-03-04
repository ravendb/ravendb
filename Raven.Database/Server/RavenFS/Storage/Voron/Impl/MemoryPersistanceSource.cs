using Voron;

namespace Raven.Database.Server.RavenFS.Storage.Voron.Impl
{
    public class MemoryPersistenceSource : IPersistenceSource
	{
		public MemoryPersistenceSource()
		{
			CreatedNew = true;
			Options = new StorageEnvironmentOptions.PureMemoryStorageEnvironmentOptions();
		}

		public StorageEnvironmentOptions Options { get; private set; }

		public bool CreatedNew { get; private set; }

		public void Dispose()
		{
			if (Options != null)
				Options.Dispose();
		}
	}
}