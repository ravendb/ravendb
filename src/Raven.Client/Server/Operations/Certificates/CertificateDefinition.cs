using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Server.Operations.Certificates
{
    public class CertificateDefinition
    {
        public bool ServerAdmin;
        public HashSet<string> Databases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        public string Thumbprint;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Thumbprint)] = Thumbprint,
                [nameof(ServerAdmin)] = ServerAdmin,
                [nameof(Databases)] = Databases
            };
        }
    }
}
