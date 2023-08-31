using System;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    [AttributeUsage(AttributeTargets.Property)]
    internal sealed class SecurityClearanceAttribute : Attribute
    {
        public SecurityClearance SecurityClearanceLevel { get; set; }

        public SecurityClearanceAttribute(SecurityClearance level)
        {
            SecurityClearanceLevel = level;
        }
    }
}
