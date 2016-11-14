using System.Collections.Generic;

namespace Raven.NewClient.Abstractions.FileSystem
{
    public class SearchResults
    {
        public List<FileHeader> Files { get; set; }
        public int FileCount { get; set; }
        public int Start { get; set; }
        public int PageSize { get; set; }
        public long DurationMilliseconds { get; set; }
    }
}
