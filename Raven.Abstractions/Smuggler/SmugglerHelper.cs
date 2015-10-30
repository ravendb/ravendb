// -----------------------------------------------------------------------
//  <copyright file="SmugglerHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Abstractions.Smuggler
{
    public static class SmugglerHelper
    {
        public static RavenJToken HandleConflictDocuments(RavenJObject metadata)
        {
            if (metadata == null)
                return null;

            if (metadata.ContainsKey(Constants.RavenReplicationConflictDocument))
                metadata.Add(Constants.RavenReplicationConflictDocumentForcePut, true);

            if (metadata.ContainsKey(Constants.RavenReplicationConflict))
                metadata.Add(Constants.RavenReplicationConflictSkipResolution, true);

            return metadata;
        }

        public static RavenJToken DisableVersioning(RavenJObject metadata)
        {
            if (metadata != null)
                metadata.Add(Constants.RavenIgnoreVersioning, true);

            return metadata;
        }
    }
}
