using System;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Migration
{
    public abstract class AbstractMigrator
    {
        protected readonly string MigrationStateKey;
        protected readonly string ServerUrl;
        protected readonly string DatabaseName;
        protected readonly HttpClient HttpClient;
        protected readonly string ApiKey;
        protected readonly bool EnableBasicAuthenticationOverUnsecuredHttp;
        protected readonly bool SkipServerCertificateValidation;
        protected readonly DatabaseItemType OperateOnTypes;
        protected readonly bool RemoveAnalyzers;
        protected readonly bool ImportRavenFs;
        protected readonly SmugglerResult Result;
        protected readonly Action<IOperationProgress> OnProgress;
        protected readonly DocumentDatabase Database;
        protected readonly OperationCancelToken CancelToken;

        protected AbstractMigrator(MigratorOptions options)
        {
            MigrationStateKey = options.MigrationStateKey;
            ServerUrl = options.ServerUrl;
            DatabaseName = options.DatabaseName;
            HttpClient = options.HttpClient;
            ApiKey = options.ApiKey;
            EnableBasicAuthenticationOverUnsecuredHttp = options.EnableBasicAuthenticationOverUnsecuredHttp;
            SkipServerCertificateValidation = options.SkipServerCertificateValidation;
            OperateOnTypes = options.OperateOnTypes;
            RemoveAnalyzers = options.RemoveAnalyzers;
            ImportRavenFs = options.ImportRavenFs;
            Result = options.Result;
            OnProgress = options.OnProgress;
            Database = options.Database;
            CancelToken = options.CancelToken;
        }

        public abstract Task Execute();

        protected async Task SaveLastOperationState(BlittableJsonReaderObject blittable)
        {
            using (var cmd = new MergedPutCommand(blittable, MigrationStateKey, null, Database))
            {
                await Database.TxMerger.Enqueue(cmd);
            }
        }
    }
}
