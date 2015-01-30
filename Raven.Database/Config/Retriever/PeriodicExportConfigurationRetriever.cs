using Raven.Abstractions.Data;

namespace Raven.Database.Config.Retriever
{
	internal class PeriodicExportConfigurationRetriever : ConfigurationRetrieverBase<PeriodicExportSetup>
	{
		public PeriodicExportConfigurationRetriever(DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
			: base(systemDatabase, localDatabase)
		{
		}

		protected override PeriodicExportSetup ApplyGlobalDocumentToLocal(PeriodicExportSetup global, PeriodicExportSetup local)
		{
			return local;
		}

		protected override PeriodicExportSetup ConvertGlobalDocumentToLocal(PeriodicExportSetup global)
		{
			return global;
		}

		public override string GetGlobalConfigurationDocumentKey(string key)
		{
			return Constants.Global.PeriodicExportDocumentName;
		}
	}
}