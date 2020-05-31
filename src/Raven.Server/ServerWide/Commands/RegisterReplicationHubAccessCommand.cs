using Raven.Client.Documents.Operations.Replication;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class RegisterReplicationHubAccessCommand : CommandBase
    {
        public string Database;
        public string HubDefinitionName;
        public readonly string CertPublicKeyHash;
        public readonly string CertThumbprint;
        public string CertificateBase64;
        public string[] AllowedReadPaths;
        public string[] AllowedWritePaths;
        public string Name;


        public RegisterReplicationHubAccessCommand()
        {
            // for deserialization
        }

        public RegisterReplicationHubAccessCommand(string databaseName, string hub, ReplicationHubAccess access, string certPublicKeyHash, string certThumbprint,
            string uniqueRequestId)
        {
            UniqueRequestId = uniqueRequestId;
            Database = databaseName;
            HubDefinitionName= hub;
            CertPublicKeyHash = certPublicKeyHash;
            CertThumbprint = certThumbprint;
            CertificateBase64 = access.CertificateBas64;
            Name = access.Name;
            AllowedReadPaths = access.AllowedReadPaths;
            AllowedWritePaths = access.AllowedWritePaths;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(Name)] = Name;
            djv[nameof(HubDefinitionName)] = HubDefinitionName;
            djv[nameof(Database)] = Database;
            djv[nameof(CertPublicKeyHash)] = CertPublicKeyHash;
            djv[nameof(CertThumbprint)] = CertThumbprint;
            djv[nameof(CertificateBase64)] = CertificateBase64;
            djv[nameof(AllowedReadPaths)] = AllowedReadPaths;
            djv[nameof(AllowedWritePaths)] = AllowedWritePaths;
            return djv;
        }

    }
}
