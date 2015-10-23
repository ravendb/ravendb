using Raven.Abstractions.Database.Smuggler.Database;

namespace Raven.Abstractions.Database.Smuggler.Other
{
    public class ExportFilesResult : LastFilesEtagsInfo
    {
        public string FilePath { get; set; }
    }
}
