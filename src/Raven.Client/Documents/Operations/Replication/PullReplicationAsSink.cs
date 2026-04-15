using System;
using System.Text;
using Raven.Client.Documents.Replication;
using Raven.Client.Exceptions.Security;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    /// <summary>
    /// Represents a pull replication task configured as a sink.
    /// This configuration defines how data is replicated from a hub to the sink and optionally from the sink back to the hub.
    /// </summary>
    public sealed class PullReplicationAsSink : ExternalReplicationBase
    {
        /// <summary>
        /// The replication mode, determining the direction of data flow between the hub and the sink.
        /// Defaults to <see cref="PullReplicationMode.HubToSink"/>.
        /// </summary>
        public PullReplicationMode Mode = PullReplicationMode.HubToSink;

        /// <summary>
        /// The paths allowed for data replication from the hub to the sink.
        /// </summary>
        public string[] AllowedHubToSinkPaths;

        /// <summary>
        /// The paths allowed for data replication from the sink to the hub.
        /// </summary>
        public string[] AllowedSinkToHubPaths;

        /// <summary>
        /// The certificate with a private key, encoded in Base64, used for secure communication.
        /// </summary>
        public string CertificateWithPrivateKey; // base64

        /// <summary>
        /// The password for the certificate, if required.
        /// </summary>
        public string CertificatePassword;

        /// <summary>
        /// The access name for the sink.
        /// </summary>
        public string AccessName;

        /// <summary>
        /// The name of the hub task that this sink replicates from.
        /// </summary>
        public string HubName;

        /// <inheritdoc cref="PullReplicationAsSink"/>
        public PullReplicationAsSink()
        {
        }

        /// <inheritdoc cref="PullReplicationAsSink"/>
        /// <param name="database">The name of the target database.</param>
        /// <param name="connectionStringName">The name of the connection string for the replication.</param>
        /// <param name="hubName">The name of the hub task that this sink replicates from.</param>
        public PullReplicationAsSink(string database, string connectionStringName, string hubName) : base(database, connectionStringName)
        {
            HubName = hubName;
        }

        public override ReplicationType GetReplicationType() => ReplicationType.PullAsSink;

        public override bool IsEqualTo(ReplicationNode other)
        {
            if (other is PullReplicationAsSink sink)
            {
                return base.IsEqualTo(other) &&
                       Mode == sink.Mode &&
                       string.Equals(HubName, sink.HubName) &&
                       string.Equals(CertificatePassword, sink.CertificatePassword) &&
                       string.Equals(CertificateWithPrivateKey, sink.CertificateWithPrivateKey);
            }

            return false;
        }

        public override ulong GetTaskKey()
        {
            var hashCode = base.GetTaskKey();
            hashCode = (hashCode * 397) ^ (ulong)Mode;
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
            djv[nameof(CertificateWithPrivateKey)] = CertificateWithPrivateKey;
            djv[nameof(CertificatePassword)] = CertificatePassword;
            djv[nameof(AllowedHubToSinkPaths)] = AllowedHubToSinkPaths;
            djv[nameof(AllowedSinkToHubPaths)] = AllowedSinkToHubPaths;
            djv[nameof(AccessName)] = AccessName;
            return djv;
        }

        public override DynamicJsonValue ToAuditJson()
        {
            var djv = base.ToAuditJson();

            djv[nameof(Mode)] = Mode;
            djv[nameof(HubName)] = HubName;
            djv[nameof(AllowedHubToSinkPaths)] = AllowedHubToSinkPaths;
            djv[nameof(AllowedSinkToHubPaths)] = AllowedSinkToHubPaths;
            djv[nameof(AccessName)] = AccessName;
            return djv;
        }

        public override string ToString()
        {
            var sb = new StringBuilder($"Replication Sink {FromString()}. " +
                                       $"Hub Task Name: '{HubName}', " +
                                       $"Connection String: '{ConnectionStringName}', " +
                                       $"Mode: '{Mode}'");

            if (string.IsNullOrEmpty(AccessName) == false)
                sb.Append($", Access Name: '{AccessName}'");

            return sb.ToString();
        }

        public override string GetDefaultTaskName() => ToString();

        internal bool HasPrivateKey()
        {
            var certBytes = Convert.FromBase64String(CertificateWithPrivateKey);

            using (var certificate = CertificateLoaderUtil.CreateCertificate(certBytes, CertificatePassword, CertificateLoaderUtil.FlagsForExport))
            {
                return certificate.HasPrivateKey;
            }
        }
    }
}
