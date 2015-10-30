// -----------------------------------------------------------------------
//  <copyright file="Authentication.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
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
    }
}
