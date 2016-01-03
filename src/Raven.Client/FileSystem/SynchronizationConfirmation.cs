namespace Raven.Abstractions.FileSystem
{
    public class SynchronizationConfirmation
    {
        public string FileName { get; set; }
        public FileStatus Status { get; set; }
    }
}
