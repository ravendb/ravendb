namespace Raven.Server.Rachis
{
    public class InstallSnapshot
    {
        public long LastIncludedIndex { get; set; }

        public long LastIncludedTerm { get; set; }

        public long SnapshotSize { get; set; }

        public long Topology { get; set; } // TODO: make this into a real topology that is being sent
    }
}