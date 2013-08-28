//-----------------------------------------------------------------------
// <copyright file="VersioningConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Bundles.Versioning.Data
{
	public class VersioningConfiguration
	{
		protected bool Equals(VersioningConfiguration other)
		{
			return string.Equals(Id, other.Id) && MaxRevisions == other.MaxRevisions && Exclude.Equals(other.Exclude);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((VersioningConfiguration) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = (Id != null ? Id.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ MaxRevisions;
				hashCode = (hashCode*397) ^ Exclude.GetHashCode();
				return hashCode;
			}
		}

		/// <summary>
		/// Id can be in the following format:
		/// 1. Raven/Versioning/{Raven-Entity-Name} - When using this format, the impacted documents are just documents with the corresponding Raven-Entity-Name metadata.
		/// 2. Raven/Versioning/DefaultConfiguration - This is a global configuration, which impacts just documents that don't have a specific Raven/Versioning/{Raven-Entity-Name} corresponding to them.
		/// </summary>
		public string Id { get; set; }
		public int MaxRevisions { get; set; }

		/// <summary>
		/// Disable versioning for the impacted document of this document.
		/// </summary>
		public bool Exclude { get; set; }
		public bool PurgeOnDelete { get; set; }

		public VersioningConfiguration()
		{
			MaxRevisions = int.MaxValue;
		}
	}
}