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

		/// <summary>
		/// Determines whether existing revisions should be deleted together with a related file. Default: false
		/// </summary>
		public bool PurgeOnDelete { get; set; }

		/// <summary>
		/// Determines if versioning should be reset on file rename. Default: 'true', means that the last existing revision will become first revision while
		/// the other ones will be deleted. If you set this option to 'false' then revisions will be renamed according to the new name of the related file.
		/// </summary>
		public bool ResetOnRename { get; set; }

		public FileVersioningConfiguration()
		{
			MaxRevisions = int.MaxValue;
			ResetOnRename = true;
		} 
	}
}