using System;
using Raven.Client.Documents.Replication;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class PullReplicationAsEdge : ExternalReplication
    {
        public string CertificateWithPrivateKey; // base64
        public string CertificatePassword;

        public string CentralPullReplicationName;
        public PullReplicationAsEdge() { }

        public PullReplicationAsEdge(string database, string connectionStringName, string centralPullReplicationName) : base(database, connectionStringName)
        {
            CentralPullReplicationName = centralPullReplicationName;
        }

        public override bool IsEqualTo(ReplicationNode other)
        {
            if (other is PullReplicationAsEdge edge)
            {
                return base.IsEqualTo(other) &&
                       string.Equals(CentralPullReplicationName, edge.CentralPullReplicationName) &&
                       string.Equals(CertificatePassword, edge.CertificatePassword) &&
                       string.Equals(CertificateWithPrivateKey, edge.CertificateWithPrivateKey);
            }
            return false;
        }

        public override ulong GetTaskKey()
        {
            var hashCode = base.GetTaskKey();
            hashCode = (hashCode * 397) ^ CalculateStringHash(CertificateWithPrivateKey);
            hashCode = (hashCode * 397) ^ CalculateStringHash(CertificatePassword);
            return (hashCode * 397) ^ CalculateStringHash(CentralPullReplicationName);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (ulong)base.GetHashCode();
                hashCode = (hashCode * 397) ^ CalculateStringHash(CertificateWithPrivateKey);
                hashCode = (hashCode * 397) ^ CalculateStringHash(CertificatePassword);
                hashCode = (hashCode * 397) ^ CalculateStringHash(CentralPullReplicationName);
                return (int)hashCode;
            }
        }

        public override DynamicJsonValue ToJson()
        {
            if (string.IsNullOrEmpty(CentralPullReplicationName))
                throw new ArgumentException("Must be not empty", nameof(CentralPullReplicationName));

            var djv = base.ToJson();

            djv[nameof(CentralPullReplicationName)] = CentralPullReplicationName;
            djv[nameof(CertificateWithPrivateKey)] = CertificateWithPrivateKey;
            djv[nameof(CertificatePassword)] = CertificatePassword;

            return djv;
        }

        public override string GetDefaultTaskName()
        {
            return $"Pull Replication as Edge from {CentralPullReplicationName}";
        }
    }
}
