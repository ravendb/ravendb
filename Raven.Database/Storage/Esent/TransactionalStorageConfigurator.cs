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
			var circularLog = GetValueFromConfiguration("Raven/Esent/CircularLog", true);
			var logFileSizeInMb = GetValueFromConfiguration("Raven/Esent/LogFileSize", 64);
			logFileSizeInMb = Math.Max(1, logFileSizeInMb/4);
			return new InstanceParameters(jetInstance)
			{
				CircularLog = circularLog,
				Recovery = true,
				NoInformationEvent = false,
				CreatePathIfNotExist = true,
				EnableIndexChecking = true,
				TempDirectory = Path.Combine(logsPath, "temp"),
				SystemDirectory = Path.Combine(logsPath, "system"),
				LogFileDirectory = Path.Combine(logsPath, "logs"),
				MaxVerPages = TranslateToSizeInVersionPages(GetValueFromConfiguration("Raven/Esent/MaxVerPages", 512), 1024 * 1024),
				PreferredVerPages = TranslateToSizeInVersionPages(GetValueFromConfiguration("Raven/Esent/PreferredVerPages", 512), 1024 * 1024),
				BaseName = "RVN",
				EventSource = "Raven",
				LogBuffers = TranslateToSizeInDatabasePages(GetValueFromConfiguration("Raven/Esent/LogBuffers", 8192), 1024),
				LogFileSize = (logFileSizeInMb * 1024),
				MaxSessions = MaxSessions,
				MaxCursors = GetValueFromConfiguration("Raven/Esent/MaxCursors", 2048),
				DbExtensionSize = TranslateToSizeInDatabasePages(GetValueFromConfiguration("Raven/Esent/DbExtensionSize", 8), 1024 * 1024),
				AlternateDatabaseRecoveryDirectory = path
			};
		}

		public void LimitSystemCache()
		{
			var defaultCacheSize = Environment.Is64BitProcess ? Math.Min(1024, (MemoryStatistics.TotalPhysicalMemory / 4)) : 256;
			int cacheSizeMaxInMegabytes = GetValueFromConfiguration("Raven/Esent/CacheSizeMax", defaultCacheSize);
			int cacheSizeMax = TranslateToSizeInDatabasePages(cacheSizeMaxInMegabytes, 1024 * 1024);
			if (SystemParameters.CacheSizeMax > cacheSizeMax)
			{
				SystemParameters.CacheSizeMax = cacheSizeMax;
			}
		}

		private static int TranslateToSizeInDatabasePages(int sizeInMegabytes, int multiply)
		{
			//This doesn't suffer from overflow, do the division first (to make the number smaller) then multiply afterwards
			double tempAmt = (double)sizeInMegabytes / SystemParameters.DatabasePageSize;
			int finalSize = (int)(tempAmt * multiply);
			return finalSize;
		}

		private static int TranslateToSizeInVersionPages(int sizeInMegabytes, int multiply)
		{
			const int JET_paramVerPageSize = 128;
			int versionPageSize = 0;
			string paramString;
			Api.JetGetSystemParameter(JET_INSTANCE.Nil, JET_SESID.Nil, (JET_param) JET_paramVerPageSize, ref versionPageSize,
			                          out paramString, 0);
			//This doesn't suffer from overflow, do the division first (to make the number smaller) then multiply afterwards
			double tempAmt = (double)sizeInMegabytes / versionPageSize;
			int finalSize = (int)(tempAmt * multiply);
			return finalSize;
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

		private bool GetValueFromConfiguration(string name, bool defaultValue)
		{
			bool value;
			if (string.IsNullOrEmpty(configuration.Settings[name]) == false &&
				bool.TryParse(configuration.Settings[name], out value))
			{
				return value;
			}
			return defaultValue;
		}
	}
}
