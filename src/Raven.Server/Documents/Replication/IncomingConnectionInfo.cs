using System;
using Raven.Client.Replication.Messages;

namespace Raven.Server.Documents.Replication
{
	public class IncomingConnectionInfo : IEquatable<IncomingConnectionInfo>
	{
		public string SourceDatabaseId { get; set; }

		public string SourceDatabaseName { get; set; }

		public string SourceUrl { get; set; }

		public string SourceMachineName { get; set; }

		public static IncomingConnectionInfo FromGetLatestEtag(ReplicationLatestEtag message)
		{
			return new IncomingConnectionInfo
			{
				SourceDatabaseName = message.SourceDatabaseName,
				SourceUrl = message.SourceUrl,
				SourceMachineName = message.SourceMachineName,
				SourceDatabaseId = message.SourceDatabaseId
			};
		}

		public override string ToString() => $"Incoming Connection Info ({nameof(SourceDatabaseId)} : {SourceDatabaseId}, {nameof(SourceDatabaseName)} : {SourceDatabaseName}, {nameof(SourceMachineName)} : {SourceMachineName})";

		public bool Equals(IncomingConnectionInfo other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return string.Equals(SourceDatabaseName, other.SourceDatabaseName, StringComparison.CurrentCultureIgnoreCase) && string.Equals(SourceUrl, other.SourceUrl, StringComparison.CurrentCultureIgnoreCase) && string.Equals(SourceMachineName, other.SourceMachineName, StringComparison.CurrentCultureIgnoreCase);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((IncomingConnectionInfo)obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				int hashCode = (SourceDatabaseName != null ? StringComparer.CurrentCultureIgnoreCase.GetHashCode(SourceDatabaseName) : 0);
				hashCode = (hashCode*397) ^ (SourceUrl != null ? StringComparer.CurrentCultureIgnoreCase.GetHashCode(SourceUrl) : 0);
				hashCode = (hashCode*397) ^ (SourceMachineName != null ? StringComparer.CurrentCultureIgnoreCase.GetHashCode(SourceMachineName) : 0);
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
	}
}