//-----------------------------------------------------------------------
// <copyright file="ReplicationConstants.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Bundles.Replication
{
	public class ReplicationConstants
	{
		public const string RavenReplicationSource = "Raven-Replication-Source";
		public const string RavenReplicationVersion = "Raven-Replication-Version";
		public const string RavenReplicationHistory = "Raven-Replication-History";
		public const string RavenReplicationVersionHiLo = "Raven/Replication/VersionHilo";
		public const string RavenReplicationConflict = "Raven-Replication-Conflict";
		public const string RavenReplicationConflictDocument = "Raven-Replication-Conflict-Document";
		public const string RavenReplicationSourcesBasePath = "Raven/Replication/Sources";
		public const string RavenReplicationDestinations = "Raven/Replication/Destinations";
		public const string RavenReplicationDestinationsBasePath = "Raven/Replication/Destinations/";

		public const int ChangeHistoryLength = 50;
	}
}
