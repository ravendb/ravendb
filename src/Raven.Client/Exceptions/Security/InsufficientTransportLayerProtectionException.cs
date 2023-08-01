using System;

namespace Raven.Client.Exceptions.Security
{
    public sealed class InsufficientTransportLayerProtectionException : SecurityException
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
