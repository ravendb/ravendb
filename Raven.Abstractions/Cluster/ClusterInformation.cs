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

		public ClusterInformation(bool isInCluster, bool isLeader)
		{
			IsInCluster = isInCluster;
			IsLeader = isInCluster && isLeader;
		}

		public bool IsInCluster { get; set; }

		public bool IsLeader { get; set; }
	}
}