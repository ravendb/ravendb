//-----------------------------------------------------------------------
// <copyright file="ValidateLicense.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using NLog;
using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Rhino.Licensing;
using Rhino.Licensing.Discovery;

namespace Raven.Database.Commercial
{
	public class ValidateLicense : IStartupTask
	{
		public static LicensingStatus CurrentLicense = new LicensingStatus
		                                        {
		                                        	Status = "AGPL - Open Source",
		                                        	Error = false,
		                                        	Message = "No license file was found.\r\n" +
		                                        	          "The AGPL license restrictions apply, only Open Source / Development work is permitted."
		                                        };

		private LicenseValidator licenseValidator;
		private readonly Logger logger = LogManager.GetCurrentClassLogger();

		public void Execute(DocumentDatabase database)
		{
			string publicKey;
			using(var stream = typeof(ValidateLicense).Assembly.GetManifestResourceStream("Raven.Database.Commercial.RavenDB.public"))
			{
				if(stream == null)
					throw new InvalidOperationException("Could not find public key for the license");
				publicKey = new StreamReader(stream).ReadToEnd();
			}
			var fullPath = Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "license.xml"));
			licenseValidator = new LicenseValidator(publicKey, fullPath)
			{
				DisableFloatingLicenses = true,
			};
			licenseValidator.LicenseInvalidated+=LicenseValidatorOnLicenseInvalidated;
			licenseValidator.MultipleLicensesWereDiscovered += LicenseValidatorOnMultipleLicensesWereDiscovered;

			if(File.Exists(fullPath) == false)
			{
				CurrentLicense = new LicensingStatus
				{
					Status = "AGPL - Open Source",
					Error = false,
					Message = "No license file was found at " + fullPath +
					          "\r\nThe AGPL license restrictions apply, only Open Source / Development work is permitted."
				};
				return;
			}

			try
			{
				licenseValidator.AssertValidLicense();

				CurrentLicense = new LicensingStatus
				{
					Status = "Commercial - " + licenseValidator.LicenseType,
					Error = false,
					Message = "Valid license " + fullPath
				};
			}
			catch (Exception e)
			{
				logger.ErrorException("Could not validate license at " + fullPath, e);

				CurrentLicense = new LicensingStatus
				{
					Status = "AGPL - Open Source",
					Error = true,
					Message = "Could not validate license file at " + fullPath + Environment.NewLine + e
				};
			}
		}

		private void LicenseValidatorOnMultipleLicensesWereDiscovered(object sender, DiscoveryHost.ClientDiscoveredEventArgs clientDiscoveredEventArgs)
		{
			logger.Error("A duplicate license was found at {0} for user {1}. Id: {2}. Both licenses were disabled!", 
				clientDiscoveredEventArgs.MachineName, 
				clientDiscoveredEventArgs.UserName, 
				clientDiscoveredEventArgs.UserId);

			CurrentLicense = new LicensingStatus
			{
				Status = "AGPL - Open Source",
				Error = true,
				Message =
					string.Format("A duplicate license was found at {0} for user {1}. Id: {2}.", clientDiscoveredEventArgs.MachineName,
					              clientDiscoveredEventArgs.UserName,
					              clientDiscoveredEventArgs.UserId)
			};
		}

		private void LicenseValidatorOnLicenseInvalidated(InvalidationType invalidationType)
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