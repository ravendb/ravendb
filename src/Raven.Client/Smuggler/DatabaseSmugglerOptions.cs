using System.Collections.Generic;

namespace Raven.Client.Smuggler
{
    public class DatabaseSmugglerOptions
    {
        public DatabaseSmugglerOptions()
        {
            OperateOnTypes = OperateOnTypes = DatabaseItemType.Indexes | DatabaseItemType.Transformers
                | DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments | DatabaseItemType.Identities;
            MaxStepsForTransformScript = 10 * 1000;
            CollectionsToExport = new List<string>();
        }

        public DatabaseItemType OperateOnTypes { get; set; }

        public int? RevisionDocumentsLimit { get; set; }

        public bool IncludeExpired { get; set; }

        public bool RemoveAnalyzers { get; set; }

        /// <summary>
        /// If true the import process will Strip replication information from documents metadata
        /// </summary>
        public bool RemoveReplicationInformation { get; set; }
        
        /// <summary>
        /// If true the import process will remove VersioningBundle from the document metadata
        /// </summary>
        public bool DisableVersioningBundle { get; set; }

        public string TransformScript { get; set; }

        public string FileName { get; set; }

        public List<string> CollectionsToExport { get; set; }

        /// <summary>
        /// Maximum number of steps that transform script can have
        /// </summary>
        public int MaxStepsForTransformScript { get; set; }

        public string Database { get; set; }
    }
}
