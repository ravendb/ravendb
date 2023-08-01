using System;
using System.Net.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Smuggler.Migration
{
    public sealed class MigratorOptions
    {
        public string MigrationStateKey { get; set; }

        public string ServerUrl { get; set; }

        public string DatabaseName { get; set; }

        public string ApiKey { get; set; }

        public string TransformScript { get; set; }

        public bool EnableBasicAuthenticationOverUnsecuredHttp { get; set; }

        public bool SkipServerCertificateValidation { get; set; }

        public bool RemoveAnalyzers { get; set; }

        public bool ImportRavenFs { get; set; }

        public DatabaseItemType OperateOnTypes { get; set; }

        public DatabaseRecordItemType OperateOnDatabaseRecordTypes { get; set; }
    }

    public sealed class MigratorParameters
    {
        public RavenHttpClient HttpClient { get; set; }

        public SmugglerResult Result { get; set; }

        public Action<IOperationProgress> OnProgress { get; set; }

        public DocumentDatabase Database { get; set; }

        public OperationCancelToken CancelToken { get; set; }
    }
}
