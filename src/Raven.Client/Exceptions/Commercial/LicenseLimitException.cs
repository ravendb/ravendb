using System;

namespace Raven.Client.Exceptions.Commercial
{
    public sealed class LicenseLimitException : RavenException
    {
        public LimitType LimitType { get; internal set; }

        public LicenseLimitException()
        {
        }

        public LicenseLimitException(string message)
            : base(message)
        {
        }

        public LicenseLimitException(LimitType limitType, string message)
            : base(message)
        {
            LimitType = limitType;
        }

        public LicenseLimitException(string message, Exception e)
            : base(message, e)
        {
        }
    }
}
