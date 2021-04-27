using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public class CertificateDefinition : CertificateMetadata
    {
        public string Certificate;
        public string Password;

        public DynamicJsonValue ToJson(bool metadataOnly = false)
        {
            var jsonValue = base.ToJson();
            if (metadataOnly == false)
            {
                jsonValue[nameof(Certificate)] = Certificate;
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
        public List<string> CollectionSecondaryKeys = new List<string>();
        public string CollectionPrimaryKey = string.Empty;
        public string PublicKeyPinningHash;

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
                [nameof(CollectionSecondaryKeys)] = CollectionSecondaryKeys,
                [nameof(CollectionPrimaryKey)] = CollectionPrimaryKey,
                [nameof(PublicKeyPinningHash)] = PublicKeyPinningHash
            };
            return jsonValue;
        }
    }

    public enum DatabaseAccess
    {
        ReadWrite = 0,
        Admin = 1,
        Read = 2
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
