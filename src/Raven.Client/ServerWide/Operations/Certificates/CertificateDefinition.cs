using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class CertificateDefinition
    {
        public string Name;
        public string Certificate;
        public string Password;
        public SecurityClearance SecurityClearance;
        public string Thumbprint;
        public string PublicKeyPinningHash;
        public DateTime NotAfter;
        public Dictionary<string, DatabaseAccess> Permissions = new Dictionary<string, DatabaseAccess>(StringComparer.OrdinalIgnoreCase);
        public string CollectionPrimaryKey = string.Empty;
        public List<string> CollectionSecondaryKeys = new List<string>();

        public DynamicJsonValue ToJson()
        {
            var permissions = new DynamicJsonValue();
            
            if (Permissions != null)
                foreach (var kvp in Permissions)
                    permissions[kvp.Key] = kvp.Value.ToString();
            
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Certificate)] = Certificate,
                [nameof(Thumbprint)] = Thumbprint,
                [nameof(PublicKeyPinningHash)] = PublicKeyPinningHash,
                [nameof(NotAfter)] = NotAfter,
                [nameof(SecurityClearance)] = SecurityClearance,
                [nameof(Permissions)] = permissions,
                [nameof(CollectionPrimaryKey)] = CollectionPrimaryKey,
                [nameof(CollectionSecondaryKeys)] = CollectionSecondaryKeys
            };
        }
    }

    public enum DatabaseAccess
    {
        ReadWrite,
        Admin
    }

    public enum SecurityClearance
    {
        UnauthenticatedClients, //Default value
        ClusterAdmin,
        ClusterNode,
        Operator,
        ValidUser
    }

    public class CertificateRawData
    {
        public byte[] RawData;
    }
}
