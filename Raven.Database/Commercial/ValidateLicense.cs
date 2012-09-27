//-----------------------------------------------------------------------
// <copyright file="ValidateLicense.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Plugins;
using Rhino.Licensing;
using Rhino.Licensing.Discovery;
using Raven.Database.Extensions;

namespace Raven.Database.Commercial
{
	public class ValidateLicense : IStartupTask
	{
		public static LicensingStatus CurrentLicense { get; set; }
		private static bool alreadyRun;
		private AbstractLicenseValidator licenseValidator;
		private readonly ILog logger = LogManager.GetCurrentClassLogger();

		static ValidateLicense()
		{
			CurrentLicense = new LicensingStatus
			{
				Status = "AGPL - Open Source",
				Error = false,
				Message = "No license file was found.\r\n" +
				          "The AGPL license restrictions apply, only Open Source / Development work is permitted."
			};
		}

		public void Execute(DocumentDatabase database)
		{
			if (alreadyRun)
				return;

			alreadyRun = true;

			string publicKey;
			using(var stream = typeof(ValidateLicense).Assembly.GetManifestResourceStream("Raven.Database.Commercial.RavenDB.public"))
			{
				if(stream == null)
					throw new InvalidOperationException("Could not find public key for the license");
				publicKey = new StreamReader(stream).ReadToEnd();
			}
			
			var licensePath = GetLicensePath(database);
			var licenseText = GetLicenseText(database);
			
			licenseValidator = new StringLicenseValidator(publicKey, licenseText)
			{
				DisableFloatingLicenses = true,
				SubscriptionEndpoint = "http://uberprof.com/Subscriptions.svc"
			};
			licenseValidator.LicenseInvalidated += OnLicenseInvalidated;
			licenseValidator.MultipleLicensesWereDiscovered += OnMultipleLicensesWereDiscovered;

			if (string.IsNullOrEmpty(licenseText))
			{
				CurrentLicense = new LicensingStatus
				{
					Status = "AGPL - Open Source",
					Error = false,
					Message = "No license file was found at " + licenseText +
					          "\r\nThe AGPL license restrictions apply, only Open Source / Development work is permitted."
				};
				return;
			}

			try
			{
				licenseValidator.AssertValidLicense(()=>
				{
					string value;

					AssertForV2(licenseValidator.LicenseAttributes, database);
					if (licenseValidator.LicenseAttributes.TryGetValue("OEM", out value) &&
						"true".Equals(value, StringComparison.InvariantCultureIgnoreCase))
					{
						licenseValidator.MultipleLicenseUsageBehavior = AbstractLicenseValidator.MultipleLicenseUsage.AllowSameLicense;
					}
				});
				

				CurrentLicense = new LicensingStatus
				{
					Status = "Commercial - " + licenseValidator.LicenseType,
					Error = false,
					Message = "Valid license at " + licensePath
				};
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not validate license at "  + licensePath + ", " + licenseText, e);

				CurrentLicense = new LicensingStatus
				{
					Status = "AGPL - Open Source",
					Error = true,
					Message = "Could not validate license: " + licensePath + ", " + licenseText + Environment.NewLine + e
				};
			}
		}

		private void AssertForV2(IDictionary<string, string> licenseAttributes, DocumentDatabase database)
		{
			string version;
			if (licenseAttributes.TryGetValue("version", out version) == false || version != "1.2")
				throw new LicenseExpiredException("This is not a licence for RavenDB 1.2");

			string maxRam;
			if (licenseAttributes.TryGetValue("maxRamUtilization", out maxRam) == false || maxRam != "unlimited")
				MemoryStatistics.MemoryLimit = int.Parse(licenseAttributes["maxRamUtilization"]);
			
			string maxParallel;
			if (licenseAttributes.TryGetValue("maxParallelism", out maxParallel) == false || maxParallel != "unlimited")
				MemoryStatistics.MaxParallelism = int.Parse(licenseAttributes["maxRamUtilization"]);
		}

		private static string GetLicenseText(DocumentDatabase database)
		{
			var value = database.Configuration.Settings["Raven/License"];
			if (string.IsNullOrEmpty(value) == false)
				return value;
			var fullPath = GetLicensePath(database).ToFullPath();
			if (File.Exists(fullPath))
				return File.ReadAllText(fullPath);
			return string.Empty;
		}

		private static string GetLicensePath(DocumentDatabase database)
		{
			var value = database.Configuration.Settings["Raven/License"];
			if (string.IsNullOrEmpty(value) == false)
				return "configuration";
			value = database.Configuration.Settings["Raven/LicensePath"];
			if (string.IsNullOrEmpty(value) == false)
				return value;
			return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "license.xml");
		}

		private void OnMultipleLicensesWereDiscovered(object sender, DiscoveryHost.ClientDiscoveredEventArgs clientDiscoveredEventArgs)
		{
			logger.Error("A duplicate license was found at {0} for user {1}. User Id: {2}. Both licenses were disabled!", 
				clientDiscoveredEventArgs.MachineName, 
				clientDiscoveredEventArgs.UserName, 
				clientDiscoveredEventArgs.UserId);

			CurrentLicense = new LicensingStatus
			{
				Status = "AGPL - Open Source",
				Error = true,
				Message =
					string.Format("A duplicate license was found at {0} for user {1}. User Id: {2}.", clientDiscoveredEventArgs.MachineName,
					              clientDiscoveredEventArgs.UserName,
					              clientDiscoveredEventArgs.UserId)
			};
		}

		private void OnLicenseInvalidated(InvalidationType invalidationType)
		{
			logger.Error("The license have expired and can no longer be used");
			CurrentLicense = new LicensingStatus
			{
				Status = "AGPL - Open Source",
				Error = true,
				Message = "License expired"
			};
		}
	}
}
