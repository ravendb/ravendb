using Xunit;

namespace Tests.Infrastructure
{
    public class LicenseRequiredTheoryAttribute : TheoryAttribute
    {
        public override string Skip
        {
            get
            {
                if (RavenFactAttribute.ShouldSkipLicense(out var skipMessage))
                    return skipMessage;

                return null;
            }
        }
    }
}
