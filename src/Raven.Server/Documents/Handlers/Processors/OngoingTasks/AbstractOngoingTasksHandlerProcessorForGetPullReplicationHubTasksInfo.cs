using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractOngoingTasksHandlerProcessorForGetPullReplicationHubTasksInfo([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract IEnumerable<OngoingTaskPullReplicationAsHub> GetOngoingTasks(TransactionOperationContext context, DatabaseRecord databaseRecord, ClusterTopology clusterTopology, long key);

        protected virtual void AssertCanExecute()
        {
            if (ResourceNameValidator.IsValidResourceName(RequestHandler.DatabaseName, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);
        }

        public override async ValueTask ExecuteAsync()
        {
            AssertCanExecute();

            var key = RequestHandler.GetLongQueryString("key");

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var clusterTopology = RequestHandler.ServerStore.GetClusterTopology(context);
                    using (var rawRecord = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.DatabaseName))
                    {
                        if (rawRecord == null)
                            throw new DatabaseDoesNotExistException(RequestHandler.DatabaseName);

                        var def = rawRecord.GetHubPullReplicationById(key);

                        if (def == null)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            return;
                        }

                        var databaseRecord = rawRecord.MaterializedRecord;

                        var currentHandlers = GetOngoingTasks(context, databaseRecord, clusterTopology, key);

                        var response = new PullReplicationDefinitionAndCurrentConnections
                        {
                            Definition = def,
                            OngoingTasks = currentHandlers.ToList()
                        };

                        await WriteResultAsync(context, response.ToJson());
                    }
                }
            }
        }

        internal async Task WriteResultAsync(JsonOperationContext context, DynamicJsonValue dynamicJsonValue)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, dynamicJsonValue);
            }
        }
    }
}
