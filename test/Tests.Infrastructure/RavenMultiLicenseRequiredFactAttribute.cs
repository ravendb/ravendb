using System;

namespace Tests.Infrastructure
{
    public class RavenMultiLicenseRequiredFactAttribute : RavenFactAttribute, Xunit.v3.IFactAttribute
    {
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

        internal static readonly bool HasLicense = RavenTestHelper.EnvironmentVariables.HasLicense
                                                   && RavenTestHelper.EnvironmentVariables.HasLicenseDeveloper
                                                   && RavenTestHelper.EnvironmentVariables.HasLicenseCommunity
                                                   && RavenTestHelper.EnvironmentVariables.HasLicenseProfessional;

        private string _skip;
        internal static string SkipMessage = $"Requires Licenses to be set via environment variable. : " +
                                             $"'{RavenTestHelper.EnvironmentVariables.LicenseKey}' - {IsSet(RavenTestHelper.EnvironmentVariables.HasLicense)} . " +
                                             $"'{RavenTestHelper.EnvironmentVariables.LicenseDeveloperKey}' - {IsSet(RavenTestHelper.EnvironmentVariables.HasLicenseDeveloper)} . " +
                                             $"'{RavenTestHelper.EnvironmentVariables.LicenseCommunityKey}' - {IsSet(RavenTestHelper.EnvironmentVariables.HasLicenseCommunity)} . " +
                                             $"'{RavenTestHelper.EnvironmentVariables.LicenseProfessionalKey}' - {IsSet(RavenTestHelper.EnvironmentVariables.HasLicenseProfessional)} . ";

        public RavenMultiLicenseRequiredFactAttribute(RavenTestCategory category) : base(category)
        {
        }

        internal static string IsSet(bool licenseSet)
        {
            return licenseSet ? "is set" : "is not set";
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
