using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Operations.Certificates
{
    public sealed class CertificateDefinition : CertificateMetadata
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
        public DateTime? NotBefore;
        public Dictionary<string, DatabaseAccess> Permissions = new Dictionary<string, DatabaseAccess>(StringComparer.OrdinalIgnoreCase);
        public List<string> CollectionSecondaryKeys = new List<string>();
        public string CollectionPrimaryKey = string.Empty;
        public string PublicKeyPinningHash;
        public bool Disabled;
        public CertificateUsage? Usage;
        public List<string> SsoServerPublicKeyPinningHashes = new List<string>();
        public bool AllowAnySsoServer;
        public List<SsoIdentifier> SsoIdentifiers = new List<SsoIdentifier>();

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
                [nameof(NotBefore)] = NotBefore,
                [nameof(CollectionSecondaryKeys)] = CollectionSecondaryKeys,
                [nameof(CollectionPrimaryKey)] = CollectionPrimaryKey,
                [nameof(PublicKeyPinningHash)] = PublicKeyPinningHash,
                [nameof(Disabled)] = Disabled,
                [nameof(Usage)] = Usage,
                [nameof(SsoServerPublicKeyPinningHashes)] = SsoServerPublicKeyPinningHashes,
                [nameof(AllowAnySsoServer)] = AllowAnySsoServer,
                [nameof(SsoIdentifiers)] = new DynamicJsonArray(SsoIdentifiers.Select(x => x.ToJson())),
            };
            return jsonValue;
        }
    }

    public enum SsoProvider
    {
        Github,
        Google,
        Microsoft,
        Windows,
    }

    public sealed class SsoIdentifier
    {
        public SsoProvider Provider;
        public string Domain;
        public string Identifier;

        public DynamicJsonValue ToJson() => new DynamicJsonValue
        {
            [nameof(Provider)] = Provider,
            [nameof(Domain)] = Domain,
            [nameof(Identifier)] = Identifier,
        };
    }

    public enum CertificateUsage
    {
        RavenServer = 0,
        RavenServerForCommunication = 1,
        Client = 2,
        SsoServer = 3,
        SsoClient = 4,
        WellKnownIssuer = 5,
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

    public sealed class CertificateRawData
    {
        public byte[] RawData;
    }
}
