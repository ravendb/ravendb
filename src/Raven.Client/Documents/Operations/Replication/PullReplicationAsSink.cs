using System;
using Raven.Client.Documents.Replication;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class PullReplicationAsSink : ExternalReplication
    {
        public string CertificateWithPrivateKey; // base64
        public string CertificatePassword;

        public string HubDefinitionName;
        public PullReplicationAsSink() { }

        public PullReplicationAsSink(string database, string connectionStringName, string hubDefinitionName) : base(database, connectionStringName)
        {
            HubDefinitionName = hubDefinitionName;
        }

        public override bool IsEqualTo(ReplicationNode other)
        {
            if (other is PullReplicationAsSink sink)
            {
                return base.IsEqualTo(other) &&
                       string.Equals(HubDefinitionName, sink.HubDefinitionName) &&
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
            return (hashCode * 397) ^ CalculateStringHash(HubDefinitionName);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (ulong)base.GetHashCode();
                hashCode = (hashCode * 397) ^ CalculateStringHash(CertificateWithPrivateKey);
                hashCode = (hashCode * 397) ^ CalculateStringHash(CertificatePassword);
                hashCode = (hashCode * 397) ^ CalculateStringHash(HubDefinitionName);
                return (int)hashCode;
            }
        }

        public override DynamicJsonValue ToJson()
        {
            if (string.IsNullOrEmpty(HubDefinitionName))
                throw new ArgumentException("Must be not empty", nameof(HubDefinitionName));

            var djv = base.ToJson();

            djv[nameof(HubDefinitionName)] = HubDefinitionName;
            djv[nameof(CertificateWithPrivateKey)] = CertificateWithPrivateKey;
            djv[nameof(CertificatePassword)] = CertificatePassword;

            return djv;
        }

        public override string GetDefaultTaskName()
        {
            return $"Pull Replication Sink from {HubDefinitionName}";
        }
    }
}
