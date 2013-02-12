// -----------------------------------------------------------------------
//  <copyright file="IndexSegmentsInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;

namespace Raven.Database.Indexing
{
	public class IndexSegmentsInfo
	{
		public long Generation { get; set; }

		public string SegmentsFileName { get; set; }

		public ICollection<string> ReferencedFiles { get; set; }

		public bool IsIndexCorrupted { get; set; }

		protected bool Equals(IndexSegmentsInfo other)
		{
			var theSameNumberOfItems = ReferencedFiles.Count == other.ReferencedFiles.Count;
			var theSameReferencedFiles = true;

			if (theSameNumberOfItems)
			{
				if (ReferencedFiles.Any(file => other.ReferencedFiles.Contains(file) == false))
					theSameReferencedFiles = false;
			}
			else
				theSameReferencedFiles = false;

			return theSameReferencedFiles &&
				Generation == other.Generation &&
				string.Equals(SegmentsFileName, other.SegmentsFileName) &&
				IsIndexCorrupted.Equals(other.IsIndexCorrupted);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = Generation.GetHashCode();
				hashCode = (hashCode * 397) ^ (SegmentsFileName != null ? SegmentsFileName.GetHashCode() : 0);
				hashCode = (hashCode * 397) ^ (ReferencedFiles != null ? ReferencedFiles.GetHashCode() : 0);
				hashCode = (hashCode * 397) ^ IsIndexCorrupted.GetHashCode();
				return hashCode;
			}
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((IndexSegmentsInfo) obj);
		}
	}
}