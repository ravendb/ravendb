using System;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database;
using Raven.Database.Plugins;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Bundles.Quotas.Documents
{
	public class DocQuotaConfiguration
	{
		private readonly DocumentDatabase database;
		private readonly long hardLimit, softLimit;
		private DateTime lastCheck;
		private VetoResult skipCheck = VetoResult.Allowed;
		private bool recheckOnDelete;

		public static DocQuotaConfiguration GetConfiguration(DocumentDatabase database)
		{
			return
				(DocQuotaConfiguration)
				database.ExtensionsState.GetOrAdd("Raven.Bundles.Quotas.DocQuotaConfiguration", s =>
				{
					var sizeQuotaConfiguration = new DocQuotaConfiguration(database);
					return sizeQuotaConfiguration;
				});
		}

		public DocQuotaConfiguration(DocumentDatabase database)
		{
			this.database = database;
			var hardLimitQuotaAsString = database.ConfigurationRetriever.GetEffectiveConfigurationSetting(Constants.DocsHardLimit);
            var softLimitQuotaAsString = database.ConfigurationRetriever.GetEffectiveConfigurationSetting(Constants.DocsSoftLimit);

			if (long.TryParse(hardLimitQuotaAsString, out hardLimit) == false)
			{
				hardLimit = long.MaxValue;
			}

			if (long.TryParse(softLimitQuotaAsString, out softLimit) == false)
			{
				softLimit = long.MaxValue;
			}
		}

		public VetoResult AllowPut()
		{
			if (hardLimit == long.MaxValue)
				return VetoResult.Allowed;

			if ((SystemTime.UtcNow - lastCheck).TotalSeconds < 30)
				return skipCheck;

			UpdateSkippedCheck();

			return skipCheck;
		}

		private void UpdateSkippedCheck()
		{
			lastCheck = SystemTime.UtcNow;

			var countOfDocuments = database.Statistics.CountOfDocuments;
			if (countOfDocuments <= softLimit)
			{
				database.Documents.Delete("Raven/Quotas/Documents", null, null);
				skipCheck = VetoResult.Allowed;
				recheckOnDelete = false;
				return;
			}

			recheckOnDelete = true;

			string msg;
			if (countOfDocuments > hardLimit) // beyond the grace margin
			{
				msg = string.Format("Database doc count is {0:#,#}, which is over the allowed quota of {1:#,#}. No more documents are allowed in.",
					countOfDocuments, hardLimit);

				database.Documents.Put("Raven/Quotas/Documents", null, new RavenJObject
				{
					{"Message", msg}
				}, new RavenJObject(), null);

				skipCheck = VetoResult.Deny(msg);
			}
			else // still before the hard limit, warn, but allow
			{
				msg = string.Format("Database doc count is {0:#,#}, which is close to the allowed quota of {1:#,#}.",
					countOfDocuments, softLimit);

				database.Documents.Put("Raven/Quotas/Documents", null, new RavenJObject
				{
					{"Message", msg}
				}, new RavenJObject(), null);
				skipCheck = VetoResult.Allowed;
			}
		}

		public void AfterDelete()
		{
			if (recheckOnDelete == false)
				return;

			UpdateSkippedCheck();
		}
	}
}