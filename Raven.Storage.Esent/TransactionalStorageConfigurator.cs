//-----------------------------------------------------------------------
// <copyright file="TransactionalStorageConfigurator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Microsoft.Isam.Esent.Interop;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;

namespace Raven.Storage.Esent
{
	public class TransactionalStorageConfigurator
	{
		public const int MaxSessions = 256;

		private readonly InMemoryRavenConfiguration configuration;

		public TransactionalStorageConfigurator(InMemoryRavenConfiguration configuration)
		{
			this.configuration = configuration;
		}

		public InstanceParameters ConfigureInstance(JET_INSTANCE jetInstance, string path)
		{
			path = Path.GetFullPath(path);
			var logsPath = path;
			if (string.IsNullOrEmpty(configuration.Settings["Raven/Esent/LogsPath"]) == false)
			{
				logsPath = configuration.Settings["Raven/Esent/LogsPath"].ToFullPath();
			}
			var logFileSizeInMb = GetValueFromConfiguration("Raven/Esent/LogFileSize", 16);
			logFileSizeInMb = Math.Max(1, logFileSizeInMb / 4);
			return new InstanceParameters(jetInstance)
			{
				CircularLog = true,
				Recovery = true,
				NoInformationEvent = false,
				CreatePathIfNotExist = true,
				TempDirectory = Path.Combine(logsPath, "temp"),
				SystemDirectory = Path.Combine(logsPath, "system"),
				LogFileDirectory = Path.Combine(logsPath, "logs"),
				MaxVerPages = TranslateToSizeInDatabasePages(GetValueFromConfiguration("Raven/Esent/MaxVerPages", 128)),
				BaseName = "RVN",
				EventSource = "Raven",
				LogBuffers = TranslateToSizeInDatabasePages(GetValueFromConfiguration("Raven/Esent/LogBuffers", 16)) / 2,
				LogFileSize = logFileSizeInMb * 1024,
				MaxSessions = MaxSessions,
				MaxCursors = GetValueFromConfiguration("Raven/Esent/MaxCursors", 2048),
				DbExtensionSize = TranslateToSizeInDatabasePages(GetValueFromConfiguration("Raven/Esent/DbExtensionSize", 16)),
				AlternateDatabaseRecoveryDirectory = path
			};
		}

		public void LimitSystemCache()
		{
			var defaultCacheSize = Environment.Is64BitProcess ? 1024 : 256;
			int cacheSizeMaxInMegabytes = GetValueFromConfiguration("Raven/Esent/CacheSizeMax",defaultCacheSize);
			int cacheSizeMax = TranslateToSizeInDatabasePages(cacheSizeMaxInMegabytes);
			if (SystemParameters.CacheSizeMax > cacheSizeMax)
			{
				SystemParameters.CacheSizeMax = cacheSizeMax;
			}
		}

		private static int TranslateToSizeInDatabasePages(int sizeInMegabytes)
		{
			return ((sizeInMegabytes * 1024) / SystemParameters.DatabasePageSize) * 1024;
		}

		private int GetValueFromConfiguration(string name, int defaultValue)
		{
			int value;
			if (string.IsNullOrEmpty(configuration.Settings[name]) == false &&
				int.TryParse(configuration.Settings[name], out value))
			{
				return value;
			}
			return defaultValue;
		}
	}
}
