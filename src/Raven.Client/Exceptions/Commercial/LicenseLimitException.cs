using System;

namespace Raven.Client.Exceptions.Commercial
{
    public class LicenseLimitException : RavenException
    {
        public LimitType Type { get; }

        public LicenseLimitException()
        {
        }

        public LicenseLimitException(string message)
            : base(message)
        {
        }

        public LicenseLimitException(LimitType type, string message)
            : base(message)
        {
            Type = type;
        }

        public LicenseLimitException(string message, Exception e)
            : base(message, e)
        {
        }
    }
}
