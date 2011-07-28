using System;
using Raven.Database;
using Raven.Database.Commercial;
using Raven.Database.Plugins;
using Raven.Abstractions.Extensions;

namespace Raven.Bundles.Quotas
{
	public class SizeQuotaConfiguration
	{
		private readonly DocumentDatabase database;
		private readonly int margin;
		private readonly long hardLimit, softLimit;
		private DateTime lastCheck;
		private VetoResult skipCheck = VetoResult.Allowed;
		private bool recheckOnDelete;

		public static SizeQuotaConfiguration GetConfiguration(DocumentDatabase database)
		{
			return
				(SizeQuotaConfiguration)
				database.ExternalState.GetOrAddAtomically("Raven.Bundles.Quotas.SizeQuotaConfiguration", s =>
				{
					var sizeQuotaConfiguration = new SizeQuotaConfiguration(database);
					return sizeQuotaConfiguration;
				});
		}

		public SizeQuotaConfiguration(DocumentDatabase database)
		{
			this.database = database;
			var hardLimitQuotaAsString = database.Configuration.Settings["Raven/Qoutas/Size/HardLimitInKB"];
			var marginAsString = database.Configuration.Settings["Raven/Qoutas/Size/GraceMarginInKB"];

			if (int.TryParse(marginAsString, out margin) == false)
				margin = 1024 * 1024;// 1 MB by default

			if (long.TryParse(hardLimitQuotaAsString, out hardLimit) == false)
			{
				softLimit = hardLimit = long.MaxValue;
			}
			else
			{
				softLimit = (hardLimit - margin)*1024; // KB -> Bytes
				hardLimit *= 1024; // KB -> Bytes
			}
		}

		public VetoResult AllowPut()
		{
			if (hardLimit == long.MaxValue)
				return VetoResult.Allowed;

			// checking the size of the database is pretty expensive, we only check it every so often, to reduce
			// its cost. This means that users might go beyond the limit, but that is okay, since the quota is soft
			// anyway
			if ((DateTime.UtcNow - lastCheck).TotalMinutes < 3)
				return skipCheck;

			UpdateSkippedCheck();

			return skipCheck;
		}

		private void UpdateSkippedCheck()
		{
			lastCheck = DateTime.UtcNow;

			var totalSizeOnDisk = database.GetTotalSizeOnDisk();
			if (totalSizeOnDisk <= softLimit)
			{
				WarningMessagesHolder.RemoveWarnings(database, "Size Qouta");
				skipCheck = VetoResult.Allowed;
				recheckOnDelete = false;
				return;
			}

			recheckOnDelete = true;

			string msg;
			if (totalSizeOnDisk > hardLimit) // beyond the grace margin
			{
				msg = string.Format("Database size is {0:#,#}kb, which is over the {1:#,#} allows qouta", totalSizeOnDisk / 1024,
									softLimit / 1024);

				WarningMessagesHolder.AddWarning(database, "Size Qouta", msg);
				skipCheck = VetoResult.Deny(msg);
			}
			else // still before the hard limit, warn, but allow
			{
				msg = string.Format("Database size is {0:#,#}kb, which is over the {1:#,#} allows qouta", totalSizeOnDisk / 1024,
											softLimit / 1024);

				WarningMessagesHolder.AddWarning(database, "Size Qouta", msg);
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