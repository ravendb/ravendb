using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Util;
using Raven.Server.Utils;
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
            X509Certificate2 certificate,
            string uniqueRequestId)
        {
            Database = databaseName;
            HubName = hubName;

            Name = access.Name;
            CertificateBase64 = access.CertificateBase64;
            AllowedHubToSinkPaths = access.AllowedHubToSinkPaths;
            AllowedSinkToHubPaths = access.AllowedSinkToHubPaths;

            if (certificate != null)
            {
                CertificatePublicKeyHash = certificate.GetPublicKeyPinningHash();
                CertificateThumbprint = certificate.Thumbprint;
                NotBefore = certificate.NotBefore;
                NotAfter = certificate.NotAfter;
                Issuer = certificate.Issuer;
                Subject = certificate.Subject;
            }

            UniqueRequestId = uniqueRequestId;
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

        public DynamicJsonValue PrepareForStorage()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(HubName)] = HubName,
                [nameof(AllowedHubToSinkPaths)] = AllowedHubToSinkPaths,
                [nameof(AllowedSinkToHubPaths)] = AllowedSinkToHubPaths,
                [nameof(NotBefore)] = NotBefore,
                [nameof(NotAfter)] = NotAfter,
                [nameof(Issuer)] = Issuer,
                [nameof(Subject)] = Subject
            };
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
