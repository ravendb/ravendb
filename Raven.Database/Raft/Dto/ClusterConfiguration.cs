// -----------------------------------------------------------------------
//  <copyright file="ClusterConfiguration.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Database.Raft.Dto
{
    public class ClusterConfiguration
    {
        public ClusterConfiguration()
        {
            EnableReplication = true;
        }
        public bool DisableReplicationStateChecks { get; set; }
        public bool EnableReplication { get; set; }

        public Dictionary<string, string> DatabaseSettings { get; set; }
    }
}
