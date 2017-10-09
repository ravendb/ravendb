using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.ServerWide;

namespace Raven.Server.Smuggler.Migration
{
    public abstract class AbstractMigrator : IDisposable
    {
        protected readonly string ServerUrl;
        protected readonly string DatabaseName;
        protected readonly SmugglerResult Result;
        protected readonly Action<IOperationProgress> OnProgress;
        protected readonly DocumentDatabase Database;
        protected readonly OperationCancelToken CancelToken;

        protected AbstractMigrator(
            string serverUrl,
            string databaseName,
            SmugglerResult result,
            Action<IOperationProgress> onProgress,
            DocumentDatabase database,
            OperationCancelToken cancelToken)
        {
            ServerUrl = serverUrl;
            DatabaseName = databaseName;
            Result = result;
            OnProgress = onProgress;
            Database = database;
            CancelToken = cancelToken;
        }

        public abstract Task Execute();

        public abstract void Dispose();
    }
}
