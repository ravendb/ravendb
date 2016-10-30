using System.Collections.Generic;

namespace Raven.Client.Smuggler
{
    public class DatabaseExportOptions
    {
        public DatabaseExportOptions()
        {
            OperateOnTypes = OperateOnTypes = DatabaseItemType.Indexes | DatabaseItemType.Transformers
                | DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments | DatabaseItemType.Identities;
            BatchSize = 1024;
            MaxStepsForTransformScript = 10*1000;
            CollectionsToExport = new List<string>();
        }

        public DatabaseItemType OperateOnTypes { get; set; }

        public int BatchSize { get; set; }

        public bool IncludeExpired { get; set; }

        public bool RemoveAnalyzers { get; set; }

        public string TransformScript { get; set; }

        public string FileName { get; set; }

        public List<string> CollectionsToExport { get; set; }

        /// <summary>
        /// Maximum number of steps that transform script can have
        /// </summary>
        public int MaxStepsForTransformScript { get; set; }
    }
}
