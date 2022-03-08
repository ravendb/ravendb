using System;
using Xunit;

namespace Tests.Infrastructure
{
    public class LicenseRequiredFactAttribute : FactAttribute
    {
        private static readonly bool HasLicense;

        internal static string SkipMessage = "Requires License to be set via 'RAVEN_LICENSE' environment variable.";

        static LicenseRequiredFactAttribute()
        {
            HasLicense = string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RAVEN_LICENSE")) == false;
        }

        public override string Skip
        {
            get
            {
                if (ShouldSkip(licenseRequired: true))
                    return SkipMessage;

                return null;
            }
        }

        internal static bool ShouldSkip(bool licenseRequired)
        {
            if (licenseRequired == false)
                return false;

            return HasLicense == false;
        }
    }
}
