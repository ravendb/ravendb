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
		private readonly long softLimit;
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
			string softLimitQuotaAsString = database.Configuration.Settings["Raven/Qoutas/Size/SoftLimitInKB"];
			string marginAsString = database.Configuration.Settings["Raven/Qoutas/Size/GraceMarginInKB"];

			if (long.TryParse(softLimitQuotaAsString, out softLimit) == false)
				softLimit = long.MaxValue;
			else
				softLimit *= 1024; // KB -> Bytes

			if (int.TryParse(marginAsString, out margin) == false)
				margin = 1024 * 1024;// 1 MB by default
		}

		public VetoResult AllowPut()
		{
			if (softLimit == long.MaxValue)
				return VetoResult.Allowed;

			// checking the size of the database is pretty expensive, we only check it every so often, to reduce
			// its cost. This means that users might go beyond the limit, but that is okay, since the quota is soft
			// anyway
			if ((DateTime.Now - lastCheck).TotalMinutes < 3)
				return skipCheck;

			UpdateSkippedCheck();

			return skipCheck;
		}

		private void UpdateSkippedCheck()
		{
			lastCheck = DateTime.Now;

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
			if (totalSizeOnDisk > softLimit + margin) // beyond the grace margin
			{
				msg = string.Format("Database size is {0:#,#}kb, which is over the {1:#,#} allows qouta", totalSizeOnDisk / 1024,
									softLimit / 1024);

				WarningMessagesHolder.AddWarning(database, "Size Qouta", msg);
				skipCheck = VetoResult.Deny(msg);
			}
			else // still in the grace period, warn, but allow
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