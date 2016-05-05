//-----------------------------------------------------------------------
// <copyright file="VersioningConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Server.Documents.Versioning
{
    public class VersioningConfiguration
    {
        public int? MaxRevisions { get; set; }

        public bool Active { get; set; }

        /// <summary>
        /// Disable versioning for the impacted document of this document unless the metadata at the time it's saved
        /// contains the key "Raven-Enable-Versioning".  This key is transient and is removed from the metadata before put.
        /// </summary>
        public bool ActiveIfExplicit { get; set; }

        public bool PurgeOnDelete { get; set; }
    }
}