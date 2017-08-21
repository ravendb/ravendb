using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class CertificateDefinition
    {
        public string Certificate;
        public bool ServerAdmin;
        public string Thumbprint;
        public Dictionary<string, DatabaseAccess> Permissions = new Dictionary<string, DatabaseAccess>(StringComparer.OrdinalIgnoreCase);

        public DynamicJsonValue ToJson()
        {
            var permissions = new DynamicJsonValue();
            foreach (var kvp in Permissions)
            {
                permissions[kvp.Key] = kvp.Value.ToString();
            }
            return new DynamicJsonValue
            {
                [nameof(Certificate)] = Certificate,
                [nameof(Thumbprint)] = Thumbprint,
                [nameof(ServerAdmin)] = ServerAdmin,
                [nameof(Permissions)] = permissions
            };
        }
    }

    public enum DatabaseAccess
    {
        ReadWrite,
        Admin
    }

    public class CertificateRawData
    {
        public byte[] RawData;
    }
}