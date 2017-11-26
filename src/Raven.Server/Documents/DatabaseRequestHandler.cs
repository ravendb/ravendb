using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications.Details;
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

        protected OperationCancelToken CreateTimeLimitedOperationToken()
        {
            return new OperationCancelToken(Database.Configuration.Databases.OperationTimeout.AsTimeSpan, Database.DatabaseShutdown);
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

        protected void AddPagingPerformanceHint(PagingOperationType operation, string action, HttpContext httpContext, int numberOfResults, int pageSize, TimeSpan duration)
        {
            if (numberOfResults <= Database.Configuration.PerformanceHints.MaxNumberOfResults)
                return;

            Database.NotificationCenter.Paging.Add(operation, action, httpContext.Request.QueryString.Value, numberOfResults, pageSize, duration);
        }
    }
}
