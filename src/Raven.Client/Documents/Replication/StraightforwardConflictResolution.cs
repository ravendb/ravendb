// -----------------------------------------------------------------------
//  <copyright file="ReplicationConfig.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Raven.Client.Documents.Replication
{
    public enum StraightforwardConflictResolution
    {
        None,
        /// <summary>
        /// Always resolve in favor of the latest version based on the last modified time
        /// </summary>
        ResolveToLatest
    }
}
