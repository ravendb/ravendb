// -----------------------------------------------------------------------
//  <copyright file="Authentication.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions;
using Raven.Database.Commercial;

namespace Raven.Database.Server.Security
{
	public static class Authentication
	{
		private static DateTime? licenseEnabled;

		public static void Disable()
		{
			licenseEnabled = null;
		}

        public static void EnableOnce()
        {
	        licenseEnabled = SystemTime.UtcNow.AddMinutes(1);
        }

		public static bool IsLicensedForRavenFs
		{
			get
			{
				if (licenseEnabled != null)
				{
					if (SystemTime.UtcNow < licenseEnabled.Value)
						return true;
					licenseEnabled = null;
				}

				string ravenFsValue;
				var license = ValidateLicense.CurrentLicense;
				if (license.IsCommercial == false)
				{
					return true; // we allow the use of ravenfs in the OSS version
				}
				if (license.Attributes.TryGetValue("ravenfs", out ravenFsValue))
				{
					bool active;
					if (bool.TryParse(ravenFsValue, out active))
						return active;
				}
				return false;
			}
		}

		public static bool IsLicensedForCounters
		{
			get
			{
				if (licenseEnabled != null)
				{
					if (SystemTime.UtcNow < licenseEnabled.Value)
						return true;
					licenseEnabled = null;
				}

				string countersValue;
				var license = ValidateLicense.CurrentLicense;
				if (license.IsCommercial == false)
				{
					return true; // we allow the use of counters in the OSS version
				}
				if (license.Attributes.TryGetValue("counters", out countersValue))
				{
					bool active;
					if (bool.TryParse(countersValue, out active))
						return active;
				}
				return false;
			}
		}

		public static bool IsLicensedForTimeSeries
		{
			get
			{
				if (licenseEnabled != null)
				{
					if (SystemTime.UtcNow < licenseEnabled.Value)
						return true;
					licenseEnabled = null;
				}

				string timeSeriesValue;
				var license = ValidateLicense.CurrentLicense;
				if (license.IsCommercial == false)
				{
					return true; // we allow the use of time series in the OSS version
				}
				if (license.Attributes.TryGetValue("timeSeries", out timeSeriesValue))
				{
					bool active;
					if (bool.TryParse(timeSeriesValue, out active))
						return active;
				}
				return false;
			}
		}

		public static bool IsEnabled
		{
			get
			{
				if (licenseEnabled != null)
                {
					if (SystemTime.UtcNow < licenseEnabled.Value)
						return true;
					licenseEnabled = null;
                }

			    return ValidateLicense.CurrentLicense.IsCommercial ||
			           ValidateLicense.CurrentLicense.ValidCommercialLicenseSeen;
			}
		}

		public static void AssertLicensedBundles(IEnumerable<string> activeBundles)
		{
			foreach (var bundle in activeBundles.Where(bundle => bundle != "PeriodicExport"))
			{
				string value;
				if (ValidateLicense.CurrentLicense.Attributes.TryGetValue(bundle, out value))
				{
					bool active;
					if (bool.TryParse(value, out active) && active == false)
						throw new InvalidOperationException("Your license does not allow the use of the " + bundle + " bundle.");
				}
			}
		}
	}
}