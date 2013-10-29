namespace Raven.Database.Storage.Voron.Impl
{
	using global::Voron;

	public class MemoryPersistanceSource : IPersistanceSource
	{
		public MemoryPersistanceSource()
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