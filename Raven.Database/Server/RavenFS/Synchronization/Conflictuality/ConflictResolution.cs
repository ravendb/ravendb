using Raven.Client.RavenFS;

namespace Raven.Database.Server.RavenFS.Synchronization.Conflictuality
{
	public class ConflictResolution
	{
		public ConflictResolutionStrategy Strategy { get; set; }
		public long Version { get; set; }
		public string RemoteServerId { get; set; }
	}
}
