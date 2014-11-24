// -----------------------------------------------------------------------
//  <copyright file="Authentication.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Database.Commercial;

namespace Raven.Database.Server.Security
{
	public static class Authentication
	{
	    private static bool isEnabled, isEnabledRavenFsForOneMinute;
		private static Timer timer;

        public static void EnableOnce()
        {
            isEnabled = true;
	        isEnabledRavenFsForOneMinute = true;

			timer = new Timer(o =>
			{
				isEnabledRavenFsForOneMinute = false;
				timer.Dispose();
			}, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        }

		public static bool IsLicensedForRavenFs
		{
			get
			{
				if (isEnabledRavenFsForOneMinute)
				{
					return true;
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
					if (Boolean.TryParse(ravenFsValue, out active))
						return active;
				}
				return false;
			}
		}

		public static bool IsEnabled
		{
			get
			{
                if (isEnabled)
                {
                    isEnabled = false;
                    return true;
                }

			    return ValidateLicense.CurrentLicense.IsCommercial ||
			           ValidateLicense.CurrentLicense.ValidCommercialLicenseSeen;
			}
		}
	}
}