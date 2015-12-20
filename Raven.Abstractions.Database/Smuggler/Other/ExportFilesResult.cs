using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Database.Smuggler.FileSystem;

namespace Raven.Abstractions.Database.Smuggler.Other
{
    // TODO arek - probably the class to remove
    public class ExportFilesResult : LastFilesEtagsInfo
    {
        public string FilePath { get; set; }
    }
}
