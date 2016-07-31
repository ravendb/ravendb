// -----------------------------------------------------------------------
//  <copyright file="ClusterInformation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Abstractions.Cluster
{
    public class ClusterInformation
    {
        public static ClusterInformation NotInCluster = new ClusterInformation(false, false);

        public ClusterInformation()
        {
        }

        public ClusterInformation(bool isInCluster, bool isLeader, bool withClusterFailoverHeader = false)
        {
            IsInCluster = isInCluster;
            IsLeader = isInCluster && isLeader;
            WithClusterFailoverHeader = withClusterFailoverHeader;
        }

        public bool IsInCluster { get; set; }

        public bool IsLeader { get; set; }

        public bool WithClusterFailoverHeader { get; set; }

        protected bool Equals(ClusterInformation other)
        {
            return IsInCluster.Equals(other.IsInCluster) 
                && IsLeader.Equals(other.IsLeader) 
                && WithClusterFailoverHeader.Equals(other.WithClusterFailoverHeader);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }
            if (ReferenceEquals(this, obj))
            {
                return true;
            }
            if (obj.GetType() != GetType())
            {
                return false;
            }
            return Equals((ClusterInformation)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return (IsInCluster.GetHashCode() * 397) ^ IsLeader.GetHashCode() ^ WithClusterFailoverHeader.GetHashCode();
            }
        }
    }
}
