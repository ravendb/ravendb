﻿using System.Collections.Specialized;
using System.Linq;
using System;
using System.IO;

using Voron;

namespace Raven.Database.Server.RavenFS.Storage.Voron.Impl
{
    public class MemoryMapPersistenceSource : IPersistenceSource
	{
		private readonly string directoryPath;

        private readonly string tempPath;

		public MemoryMapPersistenceSource(NameValueCollection settings)
		{
            if (settings == null)
                throw new ArgumentNullException("settings");

            directoryPath = AppDomain.CurrentDomain.BaseDirectory; // TODO
			var filePathFolder = new DirectoryInfo(directoryPath);
		    if (filePathFolder.Exists == false)
		        filePathFolder.Create();

            tempPath = settings["Raven/Voron/TempPath"];

			Initialize();
		}

		public StorageEnvironmentOptions Options { get; private set; }

		public bool CreatedNew { get; private set; }

		private void Initialize()
		{
			CreatedNew = Directory.EnumerateFileSystemEntries(directoryPath).Any() == false;

		    Options = StorageEnvironmentOptions.ForPath(directoryPath, tempPath);
		}

		public void Dispose()
		{
			if (Options != null)
				Options.Dispose();
		}
	}
}