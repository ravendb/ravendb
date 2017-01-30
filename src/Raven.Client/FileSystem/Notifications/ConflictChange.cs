namespace Raven.Abstractions.FileSystem.Notifications
{
    public class ConflictChange : FileSystemChange
    {
        public string FileName { get; set; }
        public string SourceServerUrl { get; set; }
        public ConflictStatus Status { get; set; }
        public FileHeader RemoteFileHeader { get; set; }
    }

    public enum ConflictStatus
    {
        Detected = 0,
        Resolved = 1
    }
}
