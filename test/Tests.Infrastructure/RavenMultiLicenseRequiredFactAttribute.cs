using System;

namespace Tests.Infrastructure
{
    public class RavenMultiLicenseRequiredFactAttribute : RavenFactAttribute, Xunit.v3.IFactAttribute
    {
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

        private static readonly bool RavenLicense;
        private static readonly bool RavenLicenseDeveloper;
        private static readonly bool RavenLicenseCommunity;
        private static readonly bool RavenLicenseProfessional;

        internal static readonly bool HasLicense;
        private string _skip;
        internal static string SkipMessage = $"Requires Licenses to be set via environment variable. : " +
                                             $"'RAVEN_LICENSE' - {IsSet(RavenLicense)} . " +
                                             $"'RAVEN_LICENSE_DEVELOPER' - {IsSet(RavenLicenseDeveloper)} . " +
                                             $"'RAVEN_LICENSE_COMMUNITY' - {IsSet(RavenLicenseCommunity)} . " +
                                             $"'RAVEN_LICENSE_PROFESSIONAL' - {IsSet(RavenLicenseProfessional)} . ";

        public RavenMultiLicenseRequiredFactAttribute(RavenTestCategory category) : base(category)
        {
        }

        internal static string IsSet(bool licenseSet)
        {
            return licenseSet ? "is set" : "is not set";
        }
        static RavenMultiLicenseRequiredFactAttribute()
        {
            RavenLicense = string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RAVEN_LICENSE")) == false;
            RavenLicenseDeveloper = string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RAVEN_LICENSE_DEVELOPER")) == false;
            RavenLicenseCommunity = string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RAVEN_LICENSE_COMMUNITY")) == false;
            RavenLicenseProfessional = string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RAVEN_LICENSE_PROFESSIONAL")) == false;

            HasLicense = RavenLicense && RavenLicenseDeveloper && RavenLicenseCommunity && RavenLicenseProfessional;
        }

        public RavenArchitecture Architecture { get; set; } = RavenArchitecture.All;

        public new string Skip
        {
            get
            {
                // Added support for the Skip option to allow temporarily skipping tests.
                // Currently used for tests affected by RavenDB-24118 until the issue is resolved.
                if (_skip != null)
                    return _skip;

                if (ShouldSkip(licenseRequired: true))
                    return SkipMessage;

                var multiPlatformSkip = RavenMultiplatformFactAttribute.ShouldSkip(RavenPlatform.All, Architecture, RavenIntrinsics.None, false, false, snowflakeRequired: false);

                if (multiPlatformSkip != null)
                    return multiPlatformSkip;

                return null;
            }
            set => _skip = value;
        }

        internal static bool ShouldSkip(bool licenseRequired)
        {
            if (licenseRequired == false)
                return false;

            return HasLicense == false;
        }
    }
}
