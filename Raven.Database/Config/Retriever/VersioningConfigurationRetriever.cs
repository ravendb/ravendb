using System;

using Raven.Abstractions.Data;
using Raven.Bundles.Versioning.Data;

namespace Raven.Database.Config.Retriever
{
	public class VersioningConfigurationRetriever : ConfigurationRetrieverBase<VersioningConfiguration>
	{
		public VersioningConfigurationRetriever(DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
			: base(systemDatabase, localDatabase)
		{
		}

		protected override VersioningConfiguration ApplyGlobalDocumentToLocal(VersioningConfiguration global, VersioningConfiguration local)
		{
			return local;
		}

		protected override VersioningConfiguration ConvertGlobalDocumentToLocal(VersioningConfiguration global)
		{
			if (string.IsNullOrEmpty(global.Id) == false) 
				global.Id = global.Id.Replace(Constants.Global.VersioningPrefix, Constants.Versioning.RavenVersioningPrefix);

			return global;
		}

		public override string GetGlobalConfigurationDocumentKey(string key)
		{
			if (string.Equals(key, Constants.Versioning.RavenVersioningDefaultConfiguration, StringComparison.OrdinalIgnoreCase))
				return Constants.Global.VersioningDefaultConfiguration;

			if (key.StartsWith(Constants.Versioning.RavenVersioningPrefix, StringComparison.OrdinalIgnoreCase))
				return key.Replace(Constants.Versioning.RavenVersioningPrefix, Constants.Global.VersioningPrefix);

			throw new NotSupportedException("Not supported key: " + key);
		}
	}
}