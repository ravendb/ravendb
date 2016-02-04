using Raven.Abstractions.FileSystem;

namespace Raven.Database.FileSystem.Synchronization.Conflictuality
{
    public class ConflictResolution
    {
        public ConflictResolutionStrategy Strategy { get; set; }
        public long Version { get; set; }
        public string RemoteServerId { get; set; }
    }
}
