namespace Raven.Database.Storage.Voron
{
	using System.IO;

	using Raven.Database.Config;

	using global::Voron.Impl;

	public class MemoryMapPersistanceSource : IPersistanceSource
	{
		private readonly InMemoryRavenConfiguration configuration;

		private readonly string directoryPath;

		private readonly string filePath;

		public MemoryMapPersistanceSource(InMemoryRavenConfiguration configuration)
		{
			this.configuration = configuration;

			directoryPath = configuration.DataDirectory;
			filePath = Path.Combine(directoryPath, "Data");

			Initialize();
		}

		public IVirtualPager Pager { get; private set; }

		public bool CreatedNew { get; private set; }

		private void Initialize()
		{
			if (Directory.Exists(directoryPath))
			{
				this.CreatedNew = !File.Exists(filePath);
			}
			else
			{
				Directory.CreateDirectory(directoryPath);
				this.CreatedNew = true;
			}

			this.Pager = new MemoryMapPager(filePath);
		}

		public void Dispose()
		{
			if (Pager != null)
				Pager.Dispose();
		}
	}
}