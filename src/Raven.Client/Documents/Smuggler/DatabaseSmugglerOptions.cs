using Raven.Client.ServerWide.JavaScript;
using Sparrow.Json;

namespace Raven.Client.Documents.Smuggler
{
    public class JavaScriptOptionsForSmuggler
    {
        public JavaScriptEngineType EngineType { get; set; } = JavaScriptEngineType.Jint;

        public bool StrictMode { get; set; } = true;

        public int MaxSteps { get; set; }  = 10 * 1000;

        public int MaxDuration { get; set; } = 1000; // TODO [shlomo] may be decreased when tests get stable

        private int TargetContextCountPerEngine { get; set; } = 10;

        private int MaxEngineCount { get; set; } = 50;
    }

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
                                                                                  DatabaseRecordItemType.QueueEtls;

        internal const DatabaseItemType OperateOnFirstShardOnly = DatabaseItemType.Indexes |
                                                              DatabaseItemType.DatabaseRecord |
                                                              DatabaseItemType.ReplicationHubCertificates |
                                                              DatabaseItemType.Identities |
                                                              DatabaseItemType.Subscriptions;


        public DatabaseSmugglerOptions()
        {
            OperateOnTypes = DefaultOperateOnTypes;
            OperateOnDatabaseRecordTypes = DefaultOperateOnDatabaseRecordTypes;
            OptionsForTransformScript = new JavaScriptOptionsForSmuggler();
            IncludeExpired = true;
            IncludeArtificial = false;
        }

        public DatabaseItemType OperateOnTypes { get; set; }

        public DatabaseRecordItemType OperateOnDatabaseRecordTypes { get; set; }

        public bool IncludeExpired { get; set; }

        public bool IncludeArtificial { get; set; }

        public bool RemoveAnalyzers { get; set; }

        public string TransformScript { get; set; }
        
        public JavaScriptOptionsForSmuggler OptionsForTransformScript { get; set; }

        public string EncryptionKey { get; set; }

        [ForceJsonSerialization]
        internal bool IsShard { get; set; }
    }

    internal interface IDatabaseSmugglerOptions
    {
        DatabaseItemType OperateOnTypes { get; set; }
        DatabaseRecordItemType OperateOnDatabaseRecordTypes { get; set; }
        bool IncludeExpired { get; set; }
        bool IncludeArtificial { get; set; }
        bool RemoveAnalyzers { get; set; }
        string TransformScript { get; set; }
        JavaScriptOptionsForSmuggler OptionsForTransformScript { get; set; }
    }
}
