﻿using System;
using Raven.Client.Documents.Replication;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class PullReplicationAsSink : ExternalReplicationBase
    {
        public string CertificateWithPrivateKey; // base64
        public string CertificatePassword;

        [Obsolete("PullReplicationAsSink.HubDefinitionName is not supported anymore. Will be removed in next major version of the product. Use HubName instead.")]
        public string HubDefinitionName { get => HubName; set => HubName = value; }

        public string HubName;

        public PullReplicationAsSink()
        {
        }

        public PullReplicationMode Mode = PullReplicationMode.HubToSink;

        public string[] AllowedHubToSinkPaths;
        public string[] AllowedSinkToHubPaths;

        public PullReplicationAsSink(string database, string connectionStringName, string hubName) : base(database, connectionStringName)
        {
            HubName = hubName;
        }

        public override bool IsEqualTo(ReplicationNode other)
        {
            if (other is PullReplicationAsSink sink)
            {
                return base.IsEqualTo(other) &&
                       string.Equals(HubName, sink.HubName) &&
                       string.Equals(CertificatePassword, sink.CertificatePassword) &&
                       string.Equals(CertificateWithPrivateKey, sink.CertificateWithPrivateKey);
            }

            return false;
        }

        public override ulong GetTaskKey()
        {
            var hashCode = base.GetTaskKey();
            hashCode = (hashCode * 397) ^ CalculateStringHash(CertificateWithPrivateKey);
            hashCode = (hashCode * 397) ^ CalculateStringHash(CertificatePassword);
            return (hashCode * 397) ^ CalculateStringHash(HubName);
        }

        public override DynamicJsonValue ToJson()
        {
            if (string.IsNullOrEmpty(HubName))
                throw new ArgumentException("Must be not empty", nameof(HubName));

            var djv = base.ToJson();

            djv[nameof(Mode)] = Mode;
            djv[nameof(HubName)] = HubName;
#pragma warning disable CS0618 // Type or member is obsolete
            djv[nameof(HubDefinitionName)] = HubDefinitionName;
#pragma warning restore CS0618 // Type or member is obsolete
            djv[nameof(CertificateWithPrivateKey)] = CertificateWithPrivateKey;
            djv[nameof(CertificatePassword)] = CertificatePassword;
            djv[nameof(AllowedHubToSinkPaths)] = AllowedHubToSinkPaths;
            djv[nameof(AllowedSinkToHubPaths)] = AllowedSinkToHubPaths;

            return djv;
        }

        public override string GetDefaultTaskName()
        {
            return $"Pull Replication Sink from {HubName}";
        }
    }
}
