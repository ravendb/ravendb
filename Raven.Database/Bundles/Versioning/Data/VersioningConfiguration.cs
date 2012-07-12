//-----------------------------------------------------------------------
// <copyright file="VersioningConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Bundles.Versioning.Data
{
	public class VersioningConfiguration
	{
		/// <summary>
		/// Id can be in the following format:
		/// 1. Raven/Versioning/{Raven-Entity-Name} - When using this format, the impacted documetns are just documents with the corresponing Raven-Entity-Name metadata.
		/// 2. Raven/Versioning/DefaultConfiguration - This is a global configuration, which impacts just documents that don't have a specifc Raven/Versioning/{Raven-Entity-Name} corresponed to them.
		/// </summary>
		public string Id { get; set; }
		public int MaxRevisions { get; set; }

		/// <summary>
		/// Disable versioning for the impacted document of this document.
		/// </summary>
		public bool Exclude { get; set; }

		public VersioningConfiguration()
		{
			MaxRevisions = int.MaxValue;
		}
	}
}