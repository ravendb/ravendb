using System;

namespace Raven.Client.Exceptions.Commercial
{
    public sealed class LicenseActivationException : RavenException
    {

        public LicenseActivationException()
        {
        }

        public LicenseActivationException(string message)
            : base(message)
        {
        }

        public LicenseActivationException(string message, Exception e)
            : base(message, e)
        {
        }
    }
}
