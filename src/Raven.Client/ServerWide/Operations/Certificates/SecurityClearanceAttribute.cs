using System;
using System.Collections.Generic;
using Raven.Client.Util;
using System.Reflection;
using Raven.Client.Exceptions.Security;
using Sparrow.Json.Parsing;

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
