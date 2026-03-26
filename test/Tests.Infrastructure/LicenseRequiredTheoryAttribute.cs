using Xunit;

namespace Tests.Infrastructure
{
    public class LicenseRequiredTheoryAttribute : TheoryAttribute, Xunit.v3.IFactAttribute
    {
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

        public new string Skip
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
