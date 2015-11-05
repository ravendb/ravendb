using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
    public class CompactStatus
    {
        public CompactStatusState State;

        public List<string> Messages { get; set; }

        public string LastProgressMessage { get; set; }

        public DateTime? LastProgressMessageTime { get; set; }

        public static string RavenDatabaseCompactStatusDocumentKey(string databaseName)
        {
            return "Raven/Database/Compact/Status/" + databaseName;
        }

        public static string RavenFilesystemCompactStatusDocumentKey(string fileSystemName)
        {
            return "Raven/FileSystem/Compact/Status/" + fileSystemName;
        }

        public static string RavenCounterStoageCompactStatusDocumentKey(string counterStorageName)
        {
            return "Raven/Counter/Compact/Status/" + counterStorageName;
        }
    }

    public enum CompactStatusState
    {
        Running, 
        Completed,
        Faulted
    }
}
