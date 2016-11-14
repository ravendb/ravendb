namespace Raven.NewClient.Abstractions.FileSystem.Notifications
{
    public class FileChangeNotification : FileSystemNotification
    {
        public string File { get; set; }

        public FileChangeAction Action { get; set; }
    }

    public enum FileChangeAction
    {
        Add,
        Delete,
        Update,
        /// <summary>
        /// This action is raised for the original file name before a rename operation
        /// </summary>
        Renaming,
        /// <summary>
        /// This action is raised for the final file name after a rename operation
        /// </summary>
        Renamed
    }
}
