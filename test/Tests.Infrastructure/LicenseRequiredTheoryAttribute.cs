using Xunit;

namespace Tests.Infrastructure
{
    public class LicenseRequiredTheoryAttribute : TheoryAttribute
    {
        public override string Skip
        {
            get
            {
                if (LicenseRequiredFactAttribute.ShouldSkip())
                    return LicenseRequiredFactAttribute.SkipMessage;

                return null;
            }
        }
    }
}
