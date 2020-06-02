using System;
using Raven.Client.Documents.Operations.Replication;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class RegisterReplicationHubAccessCommand : CommandBase
    {
        public string Database;
        public string HubDefinitionName;
        public string CertPublicKeyHash;
        public string CertThumbprint;
        public string CertificateBase64;
        public string[] AllowedReadPaths;
        public string[] AllowedWritePaths;
        public string Name;
        public DateTime NotBefore, NotAfter;
        public string Issuer;
        public string Subject;

        public RegisterReplicationHubAccessCommand()
        {
            // for deserialization
        }

        public RegisterReplicationHubAccessCommand(
            string databaseName, 
            string hub, 
            ReplicationHubAccess access, 
            string certPublicKeyHash, 
            string certThumbprint,
            string uniqueRequestId,
            string issuer,
            string subject,
            DateTime notBefore,
            DateTime notAfter)
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
            NotBefore = notBefore;
            NotAfter = notAfter;
            Issuer = issuer;
            Subject = subject;
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
            djv[nameof(NotBefore)] = NotBefore;
            djv[nameof(NotAfter)] = NotAfter;
            djv[nameof(Issuer)] = Issuer;
            djv[nameof(Subject)] = Subject;
            return djv;
        }
    }
    
    public class UnregisterReplicationHubAccessCommand : CommandBase
    {
        public string Database;
        public string HubDefinitionName;
        public string CertThumbprint;

        public UnregisterReplicationHubAccessCommand()
        {
            // for deserialization
        }

        public UnregisterReplicationHubAccessCommand(string databaseName, string hub, string certThumbprint, string uniqueRequestId)
        {
            UniqueRequestId = uniqueRequestId;
            Database = databaseName;
            HubDefinitionName= hub;
            CertThumbprint = certThumbprint;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(HubDefinitionName)] = HubDefinitionName;
            djv[nameof(Database)] = Database;
            djv[nameof(CertThumbprint)] = CertThumbprint;
            return djv;
        }
    }
}
