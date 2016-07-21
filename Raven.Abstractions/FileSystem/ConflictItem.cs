using System.Collections.Generic;

namespace Raven.Abstractions.FileSystem
{
    public class ConflictItem
    {
        /// <summary>
        /// This flag is set when remote conflict resolution
        /// was scheduled but not completed yet. 
        /// </summary>
        public bool ResolveUsingRemote { get; set; }

        public IList<HistoryItem> RemoteHistory { get; set; }

        public IList<HistoryItem> CurrentHistory { get; set; }

        public string FileName { get; set; }

        public string RemoteServerUrl { get; set; }
    }
}
