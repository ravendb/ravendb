using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors
{
    public abstract class AbstractStatsHandlerProcessorForGetDatabaseStatistics<TRequestHandler> : IDisposable
        where TRequestHandler : RequestHandler
    {
        protected readonly TRequestHandler RequestHandler;

        protected readonly HttpContext HttpContext;

        protected AbstractStatsHandlerProcessorForGetDatabaseStatistics([NotNull] TRequestHandler requestHandler)
        {
            RequestHandler = requestHandler ?? throw new ArgumentNullException(nameof(requestHandler));
            HttpContext = requestHandler.HttpContext;
        }

        protected abstract JsonOperationContext GetContext();

        protected abstract DatabaseStatistics GetDatabaseStatistics();

        protected abstract void Initialize();

        public async Task ExecuteAsync()
        {
            Initialize();

            var databaseStats = GetDatabaseStatistics();

            var context = GetContext();

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                writer.WriteDatabaseStatistics(context, databaseStats);

        }

        public virtual void Dispose()
        {
            GC.SuppressFinalize(this);
        }
    }
}
