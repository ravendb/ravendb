using System;

namespace Raven.Client.Exceptions.Security
{
    public class InsufficientTransportLayerProtectionException : SecurityException
    {
        public InsufficientTransportLayerProtectionException()
        {
        }

        public InsufficientTransportLayerProtectionException(string message) : base(message)
        {
        }

        public InsufficientTransportLayerProtectionException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
