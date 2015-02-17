using System.IO;

using Raven.Abstractions.Data;

namespace Raven.Database.Config.Retriever
{
	internal class PeriodicExportConfigurationRetriever : ConfigurationRetrieverBase<PeriodicExportSetup>
	{
		protected override PeriodicExportSetup ApplyGlobalDocumentToLocal(PeriodicExportSetup global, PeriodicExportSetup local, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			return local;
		}

		protected override PeriodicExportSetup ConvertGlobalDocumentToLocal(PeriodicExportSetup global, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			if (string.IsNullOrEmpty(global.LocalFolderName) == false)
				global.LocalFolderName = Path.Combine(global.LocalFolderName, localDatabase.Name);

			if (string.IsNullOrEmpty(global.AzureStorageContainer) == false)
			{
				global.AzureRemoteFolderName = localDatabase.Name;
			}

			if (string.IsNullOrEmpty(global.S3BucketName) == false)
			{
				global.S3RemoteFolderName = localDatabase.Name;
			}

			return global;
		}

		public override string GetGlobalConfigurationDocumentKey(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			return Constants.Global.PeriodicExportDocumentName;
		}
	}
}