using System.Collections.Generic;

namespace Raven.Client.Smuggler
{
    public class DatabaseExportOptions
    {
        public DatabaseItemType OperateOnTypes { get; set; }

        public int? BatchSize { get; set; }

        public bool ExcludeExpired { get; set; }

        public bool RemoveAnalyzers { get; set; }

        public string TransformScript { get; set; }

        public string FileName { get; set; }

        public List<string> CollectionsToExport { get; set; }
    }
}
