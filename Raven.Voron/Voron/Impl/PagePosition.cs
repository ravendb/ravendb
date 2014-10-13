// -----------------------------------------------------------------------
//  <copyright file="PagePosition.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Voron.Impl
{
	public class PagePosition
	{
		protected bool Equals(PagePosition other)
		{
			return ScratchPos == other.ScratchPos && TransactionId == other.TransactionId && JournalNumber == other.JournalNumber && IsFreedPageMarker == other.IsFreedPageMarker && ScratchNumber == other.ScratchNumber;
		}

		public override int GetHashCode()
		{
			unchecked
			{
				int hashCode = ScratchPos.GetHashCode();
				hashCode = (hashCode * 397) ^ TransactionId.GetHashCode();
				hashCode = (hashCode * 397) ^ JournalNumber.GetHashCode();
				hashCode = (hashCode * 397) ^ IsFreedPageMarker.GetHashCode();
				hashCode = (hashCode * 397) ^ ScratchNumber.GetHashCode();
				return hashCode;
			}
		}

		public long ScratchPos;
		public long TransactionId;
		public long JournalNumber;
		public int ScratchNumber;
		public bool IsFreedPageMarker;
		public bool UnusedInPTT;

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj))
				return false;
			if (ReferenceEquals(this, obj))
				return true;
			if (obj.GetType() != GetType())
				return false;

			return Equals((PagePosition)obj);
		}

		public override string ToString()
		{
			return string.Format("ScratchPos: {0}, TransactionId: {1}, JournalNumber: {2}, ScratchNumber: {3}, IsFreedPageMarker: {4}", ScratchPos, TransactionId, JournalNumber, ScratchNumber, IsFreedPageMarker);
		}
	}
}