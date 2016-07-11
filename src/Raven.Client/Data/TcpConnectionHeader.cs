using System;

namespace Raven.Abstractions.Data
{
	public class TcpConnectionHeaderMessage : IEquatable<TcpConnectionHeaderMessage>
	{
		public enum OperationTypes
		{
			None,
			BulkInsert,
			Subscription,
			Replication
		}

		public string DatabaseName { get; set; }

		public string DatabaseId { get; set; }

		public OperationTypes Operation { get; set; }

		public bool Equals(TcpConnectionHeaderMessage other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return string.Equals(DatabaseName, other.DatabaseName) && string.Equals(DatabaseId, other.DatabaseId) && Operation == other.Operation;
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((TcpConnectionHeaderMessage)obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = DatabaseName?.GetHashCode() ?? 0;
				hashCode = (hashCode*397) ^ (DatabaseId?.GetHashCode() ?? 0);
				hashCode = (hashCode*397) ^ (int)Operation;
				return hashCode;
			}
		}

		public static bool operator ==(TcpConnectionHeaderMessage left, TcpConnectionHeaderMessage right)
		{
			return Equals(left, right);
		}

		public static bool operator !=(TcpConnectionHeaderMessage left, TcpConnectionHeaderMessage right)
		{
			return !Equals(left, right);
		}
	}
}