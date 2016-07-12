using System;
using Raven.Abstractions.Data;

namespace Raven.Server.Documents.Replication
{
	public class IncomingConnectionInfo : IEquatable<IncomingConnectionInfo>
	{
		public string SourceDatabaseName { get; set; }

		public string SourceUrl { get; set; }

		public static IncomingConnectionInfo FromIncomingHeader(TcpConnectionHeaderMessage header)
		{
			return new IncomingConnectionInfo
			{
				SourceDatabaseName = header.SourceDatabaseName,
				SourceUrl = header.SourceUrl
			};
		}

		public bool Equals(IncomingConnectionInfo other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return String.Equals(SourceDatabaseName, other.SourceDatabaseName) && String.Equals(SourceUrl, other.SourceUrl);
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
				return ((SourceDatabaseName != null ? SourceDatabaseName.GetHashCode() : 0) * 397) ^ (SourceUrl != null ? SourceUrl.GetHashCode() : 0);
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