using System;
using Xunit;

namespace Tests.Infrastructure
{
    [Obsolete("Use RavenFact(RavenTestCategory.YourCategory, LicenseRequired = true) instead")]
    public class LicenseRequiredFactAttribute : FactAttribute, Xunit.v3.IFactAttribute
    {
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

        internal static readonly bool HasLicense;

        internal static string SkipMessage = "Requires License to be set via 'RAVEN_LICENSE' environment variable.";

        static LicenseRequiredFactAttribute()
        {
            HasLicense = string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("RAVEN_LICENSE")) == false;
        }

        public new string Skip
        {
            get
            {
                if (ShouldSkip())
                    return SkipMessage;

                return null;
            }
        }

        internal static bool ShouldSkip()
        {
            return HasLicense == false;
        }
    }
}
