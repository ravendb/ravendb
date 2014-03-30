using System;
using System.Collections.Specialized;
using System.IO;

using Microsoft.Isam.Esent.Interop;

using NLog;

using Raven.Database.Config;
using Raven.Database.Extensions;

namespace Raven.Database.Server.RavenFS.Storage.Esent
{
	public class StorageConfigurator
	{
		public const int MaxSessions = 256;
		private static readonly Logger log = LogManager.GetCurrentClassLogger();

		private readonly NameValueCollection settings;

		public StorageConfigurator(NameValueCollection settings)
		{
			this.settings = settings;
		}

		public void ConfigureInstance(JET_INSTANCE jetInstance, string path)
		{
			path = Path.GetFullPath(path);

			var logsPath = path;
			if (string.IsNullOrEmpty(settings["Raven/Esent/LogsPath"]) == false)
			{
				logsPath = settings["Raven/Esent/LogsPath"].ToFullPath();
			}

			var instanceParameters = new InstanceParameters(jetInstance)
			{
				CircularLog = GetValueFromConfiguration("Raven/Esent/CircularLog", true),
				Recovery = true,
				NoInformationEvent = false,
				CreatePathIfNotExist = true,
				TempDirectory = Path.Combine(logsPath, "temp"),
				SystemDirectory = Path.Combine(logsPath, "system"),
				LogFileDirectory = Path.Combine(logsPath, "logs"),
				MaxVerPages =
					TranslateToSizeInDatabasePages(GetValueFromConfiguration("Raven/Esent/MaxVerPages", 128)),
				BaseName = "RFS",
				EventSource = "RavenFS",
				LogBuffers =
					TranslateToSizeInDatabasePages(GetValueFromConfiguration("Raven/Esent/LogBuffers", 16)) / 2,
				LogFileSize = GetValueFromConfiguration("Raven/Esent/LogFileSize", 1) * 1024,
				MaxSessions = MaxSessions,
				MaxCursors = GetValueFromConfiguration("Raven/Esent/MaxCursors", 2048),
				DbExtensionSize =
					TranslateToSizeInDatabasePages(GetValueFromConfiguration("Raven/Esent/DbExtensionSize",
																			 16)),
				AlternateDatabaseRecoveryDirectory = path
			};

			log.Info(@"Esent Settings:
  MaxVerPages      = {0}
  CacheSizeMax     = {1}
  DatabasePageSize = {2}", instanceParameters.MaxVerPages, SystemParameters.CacheSizeMax,
					 SystemParameters.DatabasePageSize);
		}

		public void LimitSystemCache()
		{
			var defaultCacheSize = Environment.Is64BitProcess ? Math.Min(1024, (MemoryStatistics.TotalPhysicalMemory / 4)) : 256;
			var cacheSizeMaxInMegabytes = GetValueFromConfiguration("Raven/Esent/CacheSizeMax", defaultCacheSize);
			var cacheSizeMax = TranslateToSizeInDatabasePages(cacheSizeMaxInMegabytes);
			if (SystemParameters.CacheSizeMax > cacheSizeMax)
			{
				try
				{
					SystemParameters.CacheSizeMax = cacheSizeMax;
				}
				catch (Exception) // this case fail if we do it for the second time, we can just ignore this, then
				{
				}
			}
		}

		private static int TranslateToSizeInDatabasePages(int sizeInMegabytes)
		{
			return (sizeInMegabytes * 1024 * 1024) / SystemParameters.DatabasePageSize;
		}

		private int GetValueFromConfiguration(string name, int defaultValue)
		{
			int value;
			if (string.IsNullOrEmpty(settings[name]) == false &&
				int.TryParse(settings[name], out value))
			{
				return value;
			}
			return defaultValue;
		}

		private bool GetValueFromConfiguration(string name, bool defaultValue)
		{
			bool value;
			if (string.IsNullOrEmpty(settings[name]) == false &&
				bool.TryParse(settings[name], out value))
			{
				return value;
			}
			return defaultValue;
		}
	}
}
