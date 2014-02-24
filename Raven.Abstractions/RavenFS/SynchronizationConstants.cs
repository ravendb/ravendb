namespace Raven.Client.RavenFS
{
	public static class SynchronizationConstants
	{
		public const string RavenSynchronizationSource = "Raven-Synchronization-Source";
		public const string RavenSynchronizationVersion = "Raven-Synchronization-Version";
		public const string RavenSynchronizationHistory = "Raven-Synchronization-History";
		public const string RavenSynchronizationVersionHiLo = "Raven/Synchronization/VersionHilo";
		public const string RavenSynchronizationConflict = "Raven-Synchronization-Conflict";
		public const string RavenSynchronizationConflictResolution = "Raven-Synchronization-Conflict-Resolution";
		public const string RavenSynchronizationSourcesBasePath = "Raven/Synchronization/Sources";
		public const string RavenSynchronizationDestinations = "Raven/Synchronization/Destinations";
		public const string RavenSynchronizationDestinationsBasePath = "Raven/Synchronization/Destinations/";
		public const string RavenSynchronizationLockTimeout = "Raven-Synchronization-Lock-Timeout";
		public const string RavenSynchronizationLimit = "Raven-Synchronization-Limit";
		public const string RavenDeleteMarker = "Raven-Delete-Marker";
		public const string RavenRenameFile = "Raven-Rename-File";
		public const int ChangeHistoryLength = 50;
	}
}
