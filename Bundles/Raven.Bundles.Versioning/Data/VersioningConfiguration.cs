//-----------------------------------------------------------------------
// <copyright file="VersioningConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Bundles.Versioning.Data
{
    public class VersioningConfiguration
    {
        public string Id { get; set; }
        public int MaxRevisions { get; set; }
        public bool Exclude { get; set; }

        public VersioningConfiguration()
        {
            MaxRevisions = int.MaxValue;
        }
    }
}
