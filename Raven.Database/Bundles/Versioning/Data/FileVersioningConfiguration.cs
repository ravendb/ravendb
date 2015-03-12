// -----------------------------------------------------------------------
//  <copyright file="FileVersioningConfiguration.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Bundles.Versioning.Data
{
	public class FileVersioningConfiguration
	{
		/// <summary>
		/// TODO arek - see RavenDB-3295
		/// </summary>
		public string Id { get; set; }

		public int MaxRevisions { get; set; }

		/// <summary>
		/// Disable versioning for an impacted file of this configuration.
		/// </summary>
		public bool Exclude { get; set; }

		/// <summary>
		/// Disable versioning for an impacted file of this configuration unless the metadata at the time it's saved
		/// contains the key "Raven-Create-Version".  This key is transient and is removed from the metadata before put.
		/// </summary>
		public bool ExcludeUnlessExplicit { get; set; }

		public bool PurgeOnDelete { get; set; }

		/// <summary>
		/// It determines if existing revisions should be kept after a file rename. Default: false means that existing revisions will be deleted after the rename operation.
		/// </summary>
		public bool KeepRevisionsOnRename { get; set; }

		public FileVersioningConfiguration()
		{
			MaxRevisions = int.MaxValue;
		} 
	}
}