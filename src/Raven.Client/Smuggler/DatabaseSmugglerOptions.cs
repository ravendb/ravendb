namespace Raven.Client.Smuggler
{
    public class DatabaseSmugglerOptions
    {
        public DatabaseSmugglerOptions()
        {
            OperateOnTypes = DatabaseItemType.Indexes | DatabaseItemType.Documents | DatabaseItemType.Transformers;
        }

        public long? StartDocsEtag { get; set; }

        public DatabaseItemType OperateOnTypes { get; set; }

        public bool IgnoreErrorsAndContinue { get; set; }

        public int? DocumentsLimit { get; set; }
        public int? VersioningRevisionsLimit { get; set; }

        public string TransformScript { get; set; }

        public bool SkipConflicted { get; set; }

        public bool StripReplicationInformation { get; set; }

        public int MaxStepsForTransformScript { get; set; }

        public string Database { get; set; }
    }
}