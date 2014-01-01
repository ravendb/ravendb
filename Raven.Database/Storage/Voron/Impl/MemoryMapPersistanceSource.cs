using System.Linq;

namespace Raven.Database.Storage.Voron.Impl
{
	using System;
	using System.IO;

	using Raven.Database.Config;

	using global::Voron;
	using global::Voron.Impl;

	public class MemoryMapPersistenceSource : IPersistenceSource
	{
		private readonly InMemoryRavenConfiguration configuration;

		private readonly string directoryPath;


		public MemoryMapPersistenceSource(InMemoryRavenConfiguration configuration)
		{
			this.configuration = configuration;
			directoryPath = configuration.DataDirectory ?? AppDomain.CurrentDomain.BaseDirectory;
			var filePathFolder = new DirectoryInfo(directoryPath);
		    if (filePathFolder.Exists == false)
		        filePathFolder.Create();

			Initialize();
		}

		public StorageEnvironmentOptions Options { get; private set; }

		public bool CreatedNew { get; private set; }

		private void Initialize()
		{
			CreatedNew = Directory.EnumerateFileSystemEntries(directoryPath).Any() == false;

			Options = new StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions(directoryPath);
		}

		public void Dispose()
		{
			if (Options != null)
				Options.Dispose();
		}
	}
}