using System;

namespace Raven.Abstractions.FileSystem.Notifications
{
    public class SynchronizationUpdateNotification : FileSystemNotification
    {
        public string FileName { get; set; }
        public string DestinationFileSystemUrl { get; set; }
        public Guid SourceServerId { get; set; }
        public string SourceFileSystemUrl { get; set; }
        public SynchronizationType Type { get; set; }
        public SynchronizationAction Action { get; set; }
        public SynchronizationDirection Direction { get; set; }
    }

    public enum SynchronizationAction
    {
        Enqueue,
        Start,
        Finish
    }

    public enum SynchronizationDirection
    {
        Outgoing,
        Incoming
    }
}
