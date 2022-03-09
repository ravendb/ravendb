using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors
{
    public abstract class AbstractStatsHandlerProcessorForGetDetailedDatabaseStatistics<TRequestHandler> : IDisposable
        where TRequestHandler : RequestHandler
    {
        protected readonly TRequestHandler RequestHandler;

        protected readonly HttpContext HttpContext;

        protected AbstractStatsHandlerProcessorForGetDetailedDatabaseStatistics([NotNull] TRequestHandler requestHandler)
        {
            RequestHandler = requestHandler ?? throw new ArgumentNullException(nameof(requestHandler));
            HttpContext = requestHandler.HttpContext;
        }

        protected abstract JsonOperationContext GetContext();

        protected abstract string GetDatabaseName();

        protected abstract DetailedDatabaseStatistics GetDatabaseStatistics();

        protected abstract void Initialize();

        public async Task ExecuteAsync()
        {
            Initialize();

            var databaseName = GetDatabaseName();
            var databaseStats = GetDatabaseStatistics();
            var context = GetContext();

            GetDetailedDatabaseStatistics(databaseStats, databaseName);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                writer.WriteDetailedDatabaseStatistics(context, databaseStats);
        }

        public void GetDetailedDatabaseStatistics(DetailedDatabaseStatistics stats, string databaseName)
        {
            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
            using (serverContext.OpenReadTransaction())
            {
                stats.CountOfIdentities = RequestHandler.ServerStore.Cluster.GetNumberOfIdentities(serverContext, databaseName);
                stats.CountOfCompareExchange = RequestHandler.ServerStore.Cluster.GetNumberOfCompareExchange(serverContext, databaseName);
                stats.CountOfCompareExchangeTombstones = RequestHandler.ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(serverContext, databaseName);
            }
        }

        public virtual void Dispose()
        {
            GC.SuppressFinalize(this);
        }
    }
}
