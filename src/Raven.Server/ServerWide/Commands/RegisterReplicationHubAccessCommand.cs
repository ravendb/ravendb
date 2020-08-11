using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands
{
    public class BulkRegisterReplicationHubAccessCommand : CommandBase
    {
        public List<RegisterReplicationHubAccessCommand> Commands;
        public string Database;

        public BulkRegisterReplicationHubAccessCommand() : base(RaftIdGenerator.DontCareId)
        {
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(Database)] = Database;
            djv[nameof(Commands)] = new DynamicJsonArray(Commands.Select(x => x.ToJson(context)));
            return djv;
        }
    }

    public class RegisterReplicationHubAccessCommand : CommandBase
    {
        public string Database;
        public string HubName;
        public string CertificatePublicKeyHash;
        public string CertificateThumbprint;
        public string CertificateBase64;
        public string[] AllowedHubToSinkPaths;
        public string[] AllowedSinkToHubPaths;
        public string Name;
        public DateTime NotBefore, NotAfter;
        public string Issuer;
        public string Subject;
        public bool RegisteringSamePublicKeyPinningHash;

        public RegisterReplicationHubAccessCommand()
        {
            // for deserialization
        }

        public RegisterReplicationHubAccessCommand(
            string databaseName,
            string hubName,
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
            HubName = hubName;
            CertificatePublicKeyHash = certPublicKeyHash;
            CertificateThumbprint = certThumbprint;
            CertificateBase64 = access.CertificateBase64;
            Name = access.Name;
            AllowedHubToSinkPaths = access.AllowedHubToSinkPaths;
            AllowedSinkToHubPaths = access.AllowedSinkToHubPaths;
            NotBefore = notBefore;
            NotAfter = notAfter;
            Issuer = issuer;
            Subject = subject;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(Name)] = Name;
            djv[nameof(HubName)] = HubName;
            djv[nameof(Database)] = Database;
            djv[nameof(CertificatePublicKeyHash)] = CertificatePublicKeyHash;
            djv[nameof(CertificateThumbprint)] = CertificateThumbprint;
            djv[nameof(CertificateBase64)] = CertificateBase64;
            djv[nameof(AllowedHubToSinkPaths)] = AllowedHubToSinkPaths;
            djv[nameof(AllowedSinkToHubPaths)] = AllowedSinkToHubPaths;
            djv[nameof(NotBefore)] = NotBefore;
            djv[nameof(NotAfter)] = NotAfter;
            djv[nameof(Issuer)] = Issuer;
            djv[nameof(Subject)] = Subject;
            djv[nameof(RegisteringSamePublicKeyPinningHash)] = RegisteringSamePublicKeyPinningHash;
            return djv;
        }
    }

    public class UnregisterReplicationHubAccessCommand : CommandBase
    {
        public string Database;
        public string HubName;
        public string CertificateThumbprint;

        public UnregisterReplicationHubAccessCommand()
        {
            // for deserialization
        }

        public UnregisterReplicationHubAccessCommand(string databaseName, string hubName, string certThumbprint, string uniqueRequestId)
        {
            UniqueRequestId = uniqueRequestId;
            Database = databaseName;
            HubName = hubName;
            CertificateThumbprint = certThumbprint;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(HubName)] = HubName;
            djv[nameof(Database)] = Database;
            djv[nameof(CertificateThumbprint)] = CertificateThumbprint;
            return djv;
        }
    }
}
