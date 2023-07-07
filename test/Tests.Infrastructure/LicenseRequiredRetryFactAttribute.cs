using System;
using xRetry;

namespace Tests.Infrastructure
{
    public class LicenseRequiredRetryFactAttribute : RetryFactAttribute
    {
        public LicenseRequiredRetryFactAttribute(int maxRetries = 3, int delayBetweenRetriesMs = 0, params Type[] skipOnExceptions)
            : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
        {
        }

        public override string Skip
        {
            get
            {
                if (ShouldSkip(licenseRequired: true))
                    return LicenseRequiredFactAttribute.SkipMessage;

                return null;
            }
        }

        internal bool ShouldSkip(bool licenseRequired)
        {
            if (licenseRequired == false)
                return false;

            return LicenseRequiredFactAttribute.HasLicense == false;
        }
    }
}
