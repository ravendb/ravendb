using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class CertificateDefinition : CertificateMetadata
    {
        public string Certificate;
        public string Password;
        public string PublicKeyPinningHash;
        public string CollectionPrimaryKey = string.Empty;
        public List<string> CollectionSecondaryKeys = new List<string>();

        public DynamicJsonValue ToJson(bool metadataOnly = false)
        {
            var jsonValue = base.ToJson();
            if (metadataOnly == false)
            {
                jsonValue[nameof(Certificate)] = Certificate;
                jsonValue[nameof(PublicKeyPinningHash)] = PublicKeyPinningHash;
                jsonValue[nameof(CollectionPrimaryKey)] = CollectionPrimaryKey;
                jsonValue[nameof(CollectionSecondaryKeys)] = CollectionSecondaryKeys;
            }
            return jsonValue;
        }
    }

    public class CertificateMetadata
    {
        public string Name;
        public SecurityClearance SecurityClearance;
        public string Thumbprint;
        public DateTime? NotAfter;
        public Dictionary<string, DatabaseAccess> Permissions = new Dictionary<string, DatabaseAccess>(StringComparer.OrdinalIgnoreCase);

        public DynamicJsonValue ToJson()
        {
            var permissions = new DynamicJsonValue();
            
            if (Permissions != null)
                foreach (var kvp in Permissions)
                    permissions[kvp.Key] = kvp.Value.ToString();

            var jsonValue = new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Thumbprint)] = Thumbprint,
                [nameof(SecurityClearance)] = SecurityClearance,
                [nameof(Permissions)] = permissions,
                [nameof(NotAfter)] = NotAfter,
            };
            return jsonValue;
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
