using System;
using Raven.Client.Documents.Replication.Messages;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Replication.Stats
{
    public sealed class IncomingConnectionInfo : IEquatable<IncomingConnectionInfo>
    {
        public string SourceDatabaseId { get; set; }
        public string SourceDatabaseBase64Id { get; set; }

        public string SourceDatabaseName { get; set; }

        public string SourceUrl { get; set; }

        public string SourceMachineName { get; set; }

        public string SourceTag { get; set; }

        public string RemoteIp { get; set; }

        public ReplicationLatestEtagRequest.ReplicationType ReplicationsType { get; set; }

        public static IncomingConnectionInfo FromGetLatestEtag(ReplicationLatestEtagRequest message)
        {
            return new IncomingConnectionInfo
            {
                SourceDatabaseName = message.SourceDatabaseName,
                SourceUrl = message.SourceUrl,
                SourceMachineName = message.SourceMachineName,
                SourceDatabaseId = message.SourceDatabaseId,
                SourceDatabaseBase64Id = message.SourceDatabaseBase64Id,
                SourceTag = message.SourceTag,
                ReplicationsType = message.ReplicationsType
            };
        }

        public override string ToString() => $"Incoming Connection Info for {ReplicationsType} replication ({nameof(SourceDatabaseId)} : {SourceDatabaseId}, {nameof(SourceDatabaseName)} : {SourceDatabaseName}, {nameof(SourceMachineName)} : {SourceMachineName})";

        public string DebugInfoString() => $"{SourceTag}/{SourceDatabaseName} @ {SourceUrl}";

        public bool Equals(IncomingConnectionInfo other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(SourceDatabaseName, other.SourceDatabaseName, StringComparison.OrdinalIgnoreCase) && 
                   string.Equals(SourceUrl, other.SourceUrl, StringComparison.OrdinalIgnoreCase) && 
                   string.Equals(SourceMachineName, other.SourceMachineName, StringComparison.CurrentCultureIgnoreCase);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((IncomingConnectionInfo)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = (SourceDatabaseName != null ? StringComparer.CurrentCultureIgnoreCase.GetHashCode(SourceDatabaseName) : 0);
                hashCode = (hashCode * 397) ^ (SourceUrl != null ? StringComparer.CurrentCultureIgnoreCase.GetHashCode(SourceUrl) : 0);
                hashCode = (hashCode * 397) ^ (SourceMachineName != null ? StringComparer.CurrentCultureIgnoreCase.GetHashCode(SourceMachineName) : 0);
                return hashCode;
            }
        }

        public static bool operator ==(IncomingConnectionInfo left, IncomingConnectionInfo right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(IncomingConnectionInfo left, IncomingConnectionInfo right)
        {
            return !Equals(left, right);
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(SourceDatabaseName)] = SourceDatabaseName,
                [nameof(SourceUrl)] = SourceUrl,
                [nameof(SourceMachineName)] = SourceMachineName,
                [nameof(SourceTag)] = SourceTag,
                [nameof(RemoteIp)] = RemoteIp
            };
        }
    }
}
