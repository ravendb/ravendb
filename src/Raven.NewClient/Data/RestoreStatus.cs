using System.Collections.Generic;

namespace Raven.NewClient.Abstractions.Data
{
    public class RestoreStatus
    {
        public const string RavenRestoreStatusDocumentKey = "Raven/Restore/Status";

        public RestoreStatusState State;

        public List<string> Messages { get; set; }

        public static string RavenFilesystemRestoreStatusDocumentKey(string filesystemName)
        {
            return "Raven/FileSystem/Restore/Status/" + filesystemName;
        }
    }

    public enum RestoreStatusState
    {
        Running, 
        Completed,
        Faulted
    }
}
