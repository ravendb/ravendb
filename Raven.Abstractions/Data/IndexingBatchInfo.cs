using System;

namespace Raven.Abstractions.Data
{
	public class IndexingBatchInfo : IEquatable<IndexingBatchInfo>
	{
		public int TotalDocumentCount { get; set; }

		public long TotalDocumentSize { get; set; }

		public DateTime Timestamp { get; set; }

		public bool Equals(IndexingBatchInfo other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return TotalDocumentCount == other.TotalDocumentCount && TotalDocumentSize == other.TotalDocumentSize && Timestamp.Equals(other.Timestamp);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != GetType()) return false;
			return Equals((IndexingBatchInfo) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = TotalDocumentCount;
				hashCode = (hashCode*397) ^ TotalDocumentSize.GetHashCode();
				hashCode = (hashCode*397) ^ Timestamp.GetHashCode();
				return hashCode;
			}
		}		
	}
}