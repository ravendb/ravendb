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
				database.ExtensionsState.GetOrAddAtomically("Raven.Bundles.Quotas.DocQuotaConfiguration", s =>
				{
					var sizeQuotaConfiguration = new DocQuotaConfiguration(database);
					return sizeQuotaConfiguration;
				});
		}

		public DocQuotaConfiguration(DocumentDatabase database)
		{
			this.database = database;
			var hardLimitQuotaAsString = database.Configuration.Settings[Constants.DocsHardLimit];
			var softLimitQuotaAsString = database.Configuration.Settings[Constants.DocsSoftLimit];

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

			// checking the size of the database is pretty expensive, we only check it every so often, to reduce
			// its cost. This means that users might go beyond the limit, but that is okay, since the quota is soft
			// anyway
			if ((SystemTime.UtcNow - lastCheck).TotalMinutes < 3)
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
				database.Delete("Raven/Quotas/Documents", null, null);
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

				database.Put("Raven/Quotas/Documents", null, new RavenJObject
				{
					{"Message", msg}
				}, new RavenJObject(), null);

				skipCheck = VetoResult.Deny(msg);
			}
			else // still before the hard limit, warn, but allow
			{
				msg = string.Format("Database doc count is {0:#,#}, which is close to the allowed quota of {1:#,#}.",
					countOfDocuments, softLimit);

				database.Put("Raven/Quotas/Documents", null, new RavenJObject
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