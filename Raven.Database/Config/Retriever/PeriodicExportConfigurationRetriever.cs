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
			return global;
		}

		public override string GetGlobalConfigurationDocumentKey(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			return Constants.Global.PeriodicExportDocumentName;
		}
	}
}