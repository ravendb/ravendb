using System;

namespace Raven.Client.Exceptions.Security
{
    public class CertificateNameMismatchException : AuthenticationException
    {
        public CertificateNameMismatchException()
        {
        }

        public CertificateNameMismatchException(string message) : base(message)
        {
        }

        public CertificateNameMismatchException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
