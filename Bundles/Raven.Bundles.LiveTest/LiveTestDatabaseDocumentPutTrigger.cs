// -----------------------------------------------------------------------
//  <copyright file="LiveTestDatabaseCreationTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Configuration;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.LiveTest
{
	public class LiveTestDatabaseDocumentPutTrigger : AbstractPutTrigger
	{
		private const int QuotasHardLimitInKb = 512 * 1024;

		private const int QuotasSoftMarginInKb = (int)(0.75 * QuotasHardLimitInKb);

		public override void OnPut(string key, RavenJObject jsonReplicationDocument, RavenJObject metadata, TransactionInformation transactionInformation)
		{
			if (string.IsNullOrEmpty(Database.Name) == false && Database.Name != Constants.SystemDatabase)
				return;

			if (key.StartsWith(Constants.Database.Prefix, StringComparison.OrdinalIgnoreCase) == false)
				return;

			RavenJObject settings;
			RavenJToken value;
			if (jsonReplicationDocument.TryGetValue("Settings", out value) == false)
				jsonReplicationDocument["Settings"] = settings = new RavenJObject();
			else
				settings = (RavenJObject)value;

			EnsureQuotasBundleActivated(settings);
			EnsureVoronIsSetAsStorageEngineAndIsRunningInMemory(settings);
		}

		private static void EnsureVoronIsSetAsStorageEngineAndIsRunningInMemory(RavenJObject settings)
		{
			settings["Raven/StorageEngine"] = InMemoryRavenConfiguration.VoronTypeName;
            settings[Constants.RunInMemory] = true;
		}

		private static void EnsureQuotasBundleActivated(RavenJObject settings)
		{
			RavenJToken value;
			if (settings.TryGetValue(Constants.ActiveBundles, out value) == false)
				settings[Constants.ActiveBundles] = value = new RavenJValue(string.Empty);

			var activeBundles = value.Value<string>();
			var bundles = activeBundles.GetSemicolonSeparatedValues();

			if (bundles.Contains("Quotas", StringComparer.OrdinalIgnoreCase) == false)
				bundles.Add("Quotas");

			int hardLimitInKb;
			if (int.TryParse(ConfigurationManager.AppSettings["Raven/Bundles/LiveTest/Quotas/Size/HardLimitInKB"], out hardLimitInKb) == false) 
				hardLimitInKb = QuotasHardLimitInKb;

			int softMarginInKb;
			if (int.TryParse(ConfigurationManager.AppSettings["Raven/Bundles/LiveTest/Quotas/Size/SoftLimitInKB"], out softMarginInKb) == false)
				softMarginInKb = QuotasSoftMarginInKb;

			settings[Constants.ActiveBundles] = string.Join(";", bundles);
			settings[Constants.SizeHardLimitInKB] = hardLimitInKb;
			settings[Constants.SizeSoftLimitInKB] = softMarginInKb;
			settings[Constants.DocsHardLimit] = null;
			settings[Constants.DocsSoftLimit] = null;
		}
	}
}