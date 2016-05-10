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

        public bool PurgeOnDelete { get; set; }
    }
}