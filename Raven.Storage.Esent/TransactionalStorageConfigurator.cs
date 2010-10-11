using System.IO;
using Microsoft.Isam.Esent.Interop;
using Raven.Database;

namespace Raven.Storage.Esent
{
	public class TransactionalStorageConfigurator
	{
		public const int MaxSessions = 256;

		private readonly RavenConfiguration configuration;

		public TransactionalStorageConfigurator(RavenConfiguration configuration)
		{
			this.configuration = configuration;
		}

		public void ConfigureInstance(JET_INSTANCE jetInstance, string path)
		{
			path = Path.GetFullPath(path);
			new InstanceParameters(jetInstance)
			{
				CircularLog = true,
				Recovery = true,
				NoInformationEvent = false,
				CreatePathIfNotExist = true,
				TempDirectory = Path.Combine(path, "temp"),
				SystemDirectory = Path.Combine(path, "system"),
				LogFileDirectory = Path.Combine(path, "logs"),
				MaxVerPages = TranslateToSizeInDatabasePages(GetValueFromConfiguration("Raven/Esent/MaxVerPages", 128)),
				BaseName = "RVN",
				EventSource = "Raven",
				LogBuffers = TranslateToSizeInDatabasePages(GetValueFromConfiguration("Raven/Esent/LogBuffers", 16)) / 2,
				LogFileSize = GetValueFromConfiguration("Raven/Esent/LogFileSize", 16) * 1024,
				MaxSessions = MaxSessions,
				MaxCursors = GetValueFromConfiguration("Raven/Esent/MaxCursors", 2048),
				DbExtensionSize = TranslateToSizeInDatabasePages(GetValueFromConfiguration("Raven/Esent/DbExtensionSize", 16)),
				AlternateDatabaseRecoveryDirectory = path
			};
		}

		public void LimitSystemCache()
		{
			int cacheSizeMaxInMegabytes = GetValueFromConfiguration("Raven/Esent/CacheSizeMax",1024);
			int cacheSizeMax = TranslateToSizeInDatabasePages(cacheSizeMaxInMegabytes);
			if (SystemParameters.CacheSizeMax > cacheSizeMax)
			{
				SystemParameters.CacheSizeMax = cacheSizeMax;
			}
		}

		private static int TranslateToSizeInDatabasePages(int sizeInMegabytes)
		{
			return (sizeInMegabytes * 1024 * 1024) / SystemParameters.DatabasePageSize;
		}

		private int GetValueFromConfiguration(string name, int defaultValue)
		{
			string valueAsStr;
			int value;
			if (configuration.Settings.TryGetValue(name, out valueAsStr) &&
				int.TryParse(valueAsStr, out value))
			{
				return value;
			}
			return defaultValue;
		}
	}
}
