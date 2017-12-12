using System;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public abstract class DatabaseRequestHandler : RequestHandler
    {
        protected DocumentsContextPool ContextPool;
        protected DocumentDatabase Database;
        protected IndexStore IndexStore;
        protected Logger Logger;

        public override void Init(RequestHandlerContext context)
        {
            base.Init(context);

            Database = context.Database;
            ContextPool = Database.DocumentsStorage.ContextPool;
            IndexStore = context.Database.IndexStore;
            Logger = LoggingSource.Instance.GetLogger(Database.Name, GetType().FullName);

            var topologyEtag = GetLongFromHeaders(Constants.Headers.TopologyEtag);
            if (topologyEtag.HasValue && Database.HasTopologyChanged(topologyEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshTopology] = "true";

            var clientConfigurationEtag = GetLongFromHeaders(Constants.Headers.ClientConfigurationEtag);
            if (clientConfigurationEtag.HasValue && Database.HasClientConfigurationChanged(clientConfigurationEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshClientConfiguration] = "true";
        }

        protected async Task DatabaseConfigurations(Func<TransactionOperationContext, string,
           BlittableJsonReaderObject, Task<(long, object)>> setupConfigurationFunc,
           string debug,
           Action<string, BlittableJsonReaderObject> beforeSetupConfiguration = null,
           Action<DynamicJsonValue, BlittableJsonReaderObject, long> fillJson = null,
           HttpStatusCode statusCode = HttpStatusCode.OK)
        {
            
            if (TryGetAllowedDbs(Database.Name, out var _, requireAdmin: true) == false)
                return;

            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.EnsureNotPassive();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var configurationJson = await context.ReadForMemoryAsync(RequestBodyStream(), debug);
                beforeSetupConfiguration?.Invoke(Database.Name, configurationJson);

                var (index, _) = await setupConfigurationFunc(context, Database.Name, configurationJson);
                DatabaseRecord dbRecord;
                using (context.OpenReadTransaction())
                {
                    //TODO: maybe have a timeout here for long loading operations
                    dbRecord = ServerStore.Cluster.ReadDatabase(context, Database.Name);
                }
                if (dbRecord.Topology.RelevantFor(ServerStore.NodeTag))
                {
                    var db = await ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(Database.Name);
                    await db.RachisLogIndexNotifications.WaitForIndexNotification(index);
                }
                else
                {
                    await ServerStore.Cluster.WaitForIndexNotification(index);
                }
                HttpContext.Response.StatusCode = (int)statusCode;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var json = new DynamicJsonValue
                    {
                        ["RaftCommandIndex"] = index
                    };
                    fillJson?.Invoke(json, configurationJson, index);
                    context.Write(writer, json);
                    writer.Flush();
                }
            }
        }

        protected OperationCancelToken CreateTimeLimitedQueryOperationToken()
        {
            return new OperationCancelToken(Database.Configuration.Databases.QueryOperationTimeout.AsTimeSpan, Database.DatabaseShutdown);
        }

        protected OperationCancelToken CreateTimeLimitedDeleteDocsOperationToken()
        {
            return new OperationCancelToken(Database.Configuration.Databases.DeleteDocsOperationTimeout.AsTimeSpan, Database.DatabaseShutdown);
        }

        protected OperationCancelToken CreateTimeLimitedIndexTermsOperationToken()
        {
            return new OperationCancelToken(Database.Configuration.Databases.IndexTermsOperationTimeout.AsTimeSpan, Database.DatabaseShutdown);
        }

        protected OperationCancelToken CreateOperationToken()
        {
            return new OperationCancelToken(Database.DatabaseShutdown);
        }

        protected DisposableAction TrackRequestTime(string source, bool doPerformanceHintIfTooLong = true)
        {
            var sw = Stopwatch.StartNew();

            HttpContext.Response.OnStarting(state =>
            {
                sw.Stop();
                var httpContext = (HttpContext)state;
                httpContext.Response.Headers.Add(Constants.Headers.RequestTime, sw.ElapsedMilliseconds.ToString());
                return Task.CompletedTask;
            }, HttpContext);

            if (doPerformanceHintIfTooLong == false)
                return null;

            return new DisposableAction(() =>
            {
                if (sw.Elapsed <= Database.Configuration.PerformanceHints.TooLongRequestThreshold.AsTimeSpan)
                    return;

                try
                {
                    Database
                        .NotificationCenter
                        .RequestLatency
                        .AddHint(HttpContext.Request.Path,HttpContext.Request.Query, sw.ElapsedMilliseconds, source);
                }
                catch (Exception e)
                {
                    //precaution - should never arrive here
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Failed to write request time in response headers. This is not supposed to happen and is probably a bug. The request path was: {HttpContext.Request.Path}", e);

                    throw;
                }
            });
        }

        protected void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, int numberOfResults, int pageSize, TimeSpan duration)
        {
            if (numberOfResults <= Database.Configuration.PerformanceHints.MaxNumberOfResults)
                return;

            Database.NotificationCenter.Paging.Add(operation, action, details, numberOfResults, pageSize, duration);
        }
    }
}
