using System;
using System.Collections.Generic;
using Raven.Client.Util;
using System.Reflection;
using Raven.Client.Exceptions.Security;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.Certificates
{

    public enum AuthenticationStatus
    {
        None,
        NoCertificateProvided,
        UnfamiliarCertificate,
        UnfamiliarIssuer,
        Allowed,
        Operator,
        ClusterAdmin,
        Expired,
        NotYetValid
    }
    public class SecurityClearanceAttribute : Attribute
    {
        public AuthenticationStatus SecurityClearanceLevel { get; set; }
        public SecurityClearanceAttribute(AuthenticationStatus name)
        {
            SecurityClearanceLevel = name;
        }
    }
}
