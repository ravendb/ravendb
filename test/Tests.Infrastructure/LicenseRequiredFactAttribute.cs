using System;
using Xunit;

namespace Tests.Infrastructure
{
    [Obsolete("Use RavenFact(RavenTestCategory.YourCategory, LicenseRequired = true) instead")]
    public class LicenseRequiredFactAttribute : FactAttribute, Xunit.v3.IFactAttribute
    {
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

        internal static string SkipMessage = $"Requires License to be set via '{RavenTestHelper.EnvironmentVariables.LicenseKey}' environment variable.";

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
            return RavenTestHelper.EnvironmentVariables.HasLicense == false;
        }
    }
}
