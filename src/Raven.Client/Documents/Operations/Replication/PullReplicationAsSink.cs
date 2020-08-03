using System;
using Raven.Client.Documents.Replication;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    public class PullReplicationAsSink : ExternalReplicationBase
    {
        public string CertificateWithPrivateKey; // base64
        public string CertificatePassword;

        public string _hubName;
        public PullReplicationAsSink() { }

        public PullReplicationMode Mode = PullReplicationMode.Outgoing;

        public string[] AllowedWritePaths;
        public string[] AllowedReadPaths;

        public PullReplicationAsSink(string database, string connectionStringName, string hubName) : base(database, connectionStringName)
        {
            _hubName = hubName;
        }

        public override bool IsEqualTo(ReplicationNode other)
        {
            if (other is PullReplicationAsSink sink)
            {
                return base.IsEqualTo(other) &&
                       string.Equals(_hubName, sink._hubName) &&
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
            return (hashCode * 397) ^ CalculateStringHash(_hubName);
        }

        public override DynamicJsonValue ToJson()
        {
            if (string.IsNullOrEmpty(_hubName))
                throw new ArgumentException("Must be not empty", nameof(_hubName));

            var djv = base.ToJson();

            djv[nameof(Mode)] = Mode;
            djv[nameof(_hubName)] = _hubName;
            djv[nameof(CertificateWithPrivateKey)] = CertificateWithPrivateKey;
            djv[nameof(CertificatePassword)] = CertificatePassword;
            djv[nameof(AllowedWritePaths)] = AllowedWritePaths;
            djv[nameof(AllowedReadPaths)] = AllowedReadPaths;

            return djv;
        }

        public override string GetDefaultTaskName()
        {
            return $"Pull Replication Sink from {_hubName}";
        }
    }
}
