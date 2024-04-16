using System;
using Xunit;

namespace Tests.Infrastructure
{
    public class MultiLicenseRequiredFactAttribute : FactAttribute
    {
        private static readonly bool RavenLicense;
        private static readonly bool RavenLicenseDeveloper;
        private static readonly bool RavenLicenseCommunity;
        private static readonly bool RavenLicenseProfessional;

        internal static readonly bool HasLicense;

        internal static string SkipMessage = $"Requires Licenses to be set via environment variable. : " +
                                             $"'RAVEN_LICENSE' - {IsSet(RavenLicense)} . " +
                                             $"'RAVEN_LICENSE_DEVELOPER' - {IsSet(RavenLicenseDeveloper)} . " +
                                             $"'RAVEN_LICENSE_COMMUNITY' - {IsSet(RavenLicenseCommunity)} . " +
                                             $"'RAVEN_LICENSE_PROFESSIONAL' - {IsSet(RavenLicenseProfessional)} . ";

        internal static string IsSet(bool licenseSet)
        {
            return licenseSet ? "is set" : "is not set";
        }
        static MultiLicenseRequiredFactAttribute()
        {
            RavenLicense = string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RAVEN_LICENSE")) == false;
            RavenLicenseDeveloper = string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RAVEN_LICENSE_DEVELOPER")) == false;
            RavenLicenseCommunity = string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RAVEN_LICENSE_COMMUNITY")) == false;
            RavenLicenseProfessional = string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RAVEN_LICENSE_PROFESSIONAL")) == false;

            HasLicense = RavenLicense && RavenLicenseDeveloper && RavenLicenseCommunity && RavenLicenseProfessional;
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
