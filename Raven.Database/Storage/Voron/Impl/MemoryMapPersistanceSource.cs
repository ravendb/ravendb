using Amazon.ImportExport.Model;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Database.Storage.Voron.Impl
{
	using System;
	using System.IO;

	using Raven.Database.Config;

	using global::Voron;
	using global::Voron.Impl;

	public class MemoryMapPersistenceSource : IPersistenceSource
	{
		private const string PAGER_FILENAME = "Raven.voron";

		private readonly string directoryPath;

		private readonly string filePath;

		public MemoryMapPersistenceSource(InMemoryRavenConfiguration configuration)
		{
			if(configuration == null)
				throw new ArgumentNullException("configuration");

			var allowIncrementalBackupsSetting = configuration.Settings["Raven/Voron/AllowIncrementalBackups"] ?? "false";
			if (!allowIncrementalBackupsSetting.Equals("true",StringComparison.OrdinalIgnoreCase) &&
				!allowIncrementalBackupsSetting.Equals("false", StringComparison.OrdinalIgnoreCase))
				throw new ArgumentException("Raven/Voron/AllowIncrementalBackups settings key contains invalid value");


			directoryPath = configuration.DataDirectory ?? AppDomain.CurrentDomain.BaseDirectory;
            filePath = directoryPath;
		    var filePathFolder = new DirectoryInfo(filePath);
		    if (filePathFolder.Exists == false)
		        filePathFolder.Create();

			Initialize(Convert.ToBoolean(allowIncrementalBackupsSetting));
		}

		public StorageEnvironmentOptions Options { get; private set; }

		public bool CreatedNew { get; private set; }

		private void Initialize(bool allowIncrementalBackups)
		{
			var storageFilePath = Path.Combine(filePath, PAGER_FILENAME);

			if (Directory.Exists(directoryPath))
			{
				CreatedNew = !File.Exists(storageFilePath);
			}
			else
			{
				Directory.CreateDirectory(directoryPath);
				CreatedNew = true;
			}

			Options = new StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions(directoryPath)
			{
				IncrementalBackupEnabled = allowIncrementalBackups
			};

		}

		public void Dispose()
		{
			if (Options != null)
				Options.Dispose();
		}
	}
}