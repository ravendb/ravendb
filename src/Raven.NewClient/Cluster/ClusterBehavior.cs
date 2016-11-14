// -----------------------------------------------------------------------
//  <copyright file="ClusterBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.NewClient.Abstractions.Cluster
{
    public enum ClusterBehavior
    {
        ReadFromLeaderWriteToLeader = 1,
        ReadFromLeaderWriteToLeaderWithFailovers = 2,
        ReadFromAllWriteToLeader = 3,
        ReadFromAllWriteToLeaderWithFailovers = 4,
        None = 0
    }
}
