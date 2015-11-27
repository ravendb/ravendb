using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Database.Smuggler.FileSystem;

namespace Raven.Abstractions.Database.Smuggler.Other
{
    public class ExportFilesResult : LastFilesEtagsInfo
    {
        public string FilePath { get; set; }
    }
}
