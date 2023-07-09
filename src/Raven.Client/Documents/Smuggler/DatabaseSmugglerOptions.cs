using System.Collections.Generic;

namespace Raven.Client.Documents.Smuggler
{
    public class DatabaseSmugglerOptions : IDatabaseSmugglerOptions
    {
        public const DatabaseItemType DefaultOperateOnTypes = DatabaseItemType.Indexes |
                                                              DatabaseItemType.Documents |
                                                              DatabaseItemType.RevisionDocuments |
                                                              DatabaseItemType.Conflicts |
                                                              DatabaseItemType.DatabaseRecord |
                                                              DatabaseItemType.ReplicationHubCertificates |
                                                              DatabaseItemType.Identities |
                                                              DatabaseItemType.CompareExchange |
                                                              DatabaseItemType.Attachments |
                                                              DatabaseItemType.CounterGroups |
                                                              DatabaseItemType.Subscriptions |
                                                              DatabaseItemType.TimeSeries;

        public const DatabaseRecordItemType DefaultOperateOnDatabaseRecordTypes = DatabaseRecordItemType.Client |
                                                                                  DatabaseRecordItemType.ConflictSolverConfig |
                                                                                  DatabaseRecordItemType.Expiration |
                                                                                  DatabaseRecordItemType.ExternalReplications |
                                                                                  DatabaseRecordItemType.PeriodicBackups |
                                                                                  DatabaseRecordItemType.RavenConnectionStrings |
                                                                                  DatabaseRecordItemType.RavenEtls |
                                                                                  DatabaseRecordItemType.Revisions |
                                                                                  DatabaseRecordItemType.Settings |
                                                                                  DatabaseRecordItemType.SqlConnectionStrings |
                                                                                  DatabaseRecordItemType.Sorters |
                                                                                  DatabaseRecordItemType.SqlEtls |
                                                                                  DatabaseRecordItemType.HubPullReplications |
                                                                                  DatabaseRecordItemType.SinkPullReplications |
                                                                                  DatabaseRecordItemType.TimeSeries |
                                                                                  DatabaseRecordItemType.DocumentsCompression |
                                                                                  DatabaseRecordItemType.Analyzers |
                                                                                  DatabaseRecordItemType.LockMode |
                                                                                  DatabaseRecordItemType.OlapConnectionStrings |
                                                                                  DatabaseRecordItemType.OlapEtls |
                                                                                  DatabaseRecordItemType.ElasticSearchConnectionStrings |
                                                                                  DatabaseRecordItemType.ElasticSearchEtls |
                                                                                  DatabaseRecordItemType.PostgreSQLIntegration |
                                                                                  DatabaseRecordItemType.QueueConnectionStrings |
                                                                                  DatabaseRecordItemType.QueueEtls |
                                                                                  DatabaseRecordItemType.IndexesHistory |
                                                                                  DatabaseRecordItemType.Refresh;

        private const int DefaultMaxStepsForTransformScript = 10 * 1000;

        public DatabaseSmugglerOptions()
        {
            OperateOnTypes = DefaultOperateOnTypes;
            OperateOnDatabaseRecordTypes = DefaultOperateOnDatabaseRecordTypes;
            MaxStepsForTransformScript = DefaultMaxStepsForTransformScript;
            IncludeExpired = true;
            IncludeArtificial = false;
            Collections = new List<string>();
        }

        public DatabaseItemType OperateOnTypes { get; set; }

        public DatabaseRecordItemType OperateOnDatabaseRecordTypes { get; set; }

        public bool IncludeExpired { get; set; }

        public bool IncludeArtificial { get; set; }

        public bool RemoveAnalyzers { get; set; }

        public string TransformScript { get; set; }

        public int MaxStepsForTransformScript { get; set; }

        public string EncryptionKey { get; set; }

        public List<string> Collections { get; set; }

        /// <summary>
        /// In case the database is corrupted (for example, Compression Dictionaries are lost), it is possible to export all the remaining data.
        /// If '<c>true</c>', the process will continue in the presence of corrupted data, with an entry of error data into the export result report.
        /// If '<c>false</c>', the process of export will be interrupted in the presence of corrupted data. 
        /// The default value is '<c>false</c>'.
        /// </summary>
        public bool SkipCorruptedData { get; set; }
    }

    internal interface IDatabaseSmugglerOptions
    {
        DatabaseItemType OperateOnTypes { get; set; }
        DatabaseRecordItemType OperateOnDatabaseRecordTypes { get; set; }
        bool IncludeExpired { get; set; }
        bool IncludeArtificial { get; set; }
        bool RemoveAnalyzers { get; set; }
        string TransformScript { get; set; }
        int MaxStepsForTransformScript { get; set; }
        List<string> Collections { get; set; }
    }

    public enum ExportCompressionAlgorithm
    {
        Zstd,
        Gzip
    }
}
