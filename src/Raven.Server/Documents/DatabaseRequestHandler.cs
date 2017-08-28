using System;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Microsoft.AspNetCore.Http;
using Raven.Client;
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
            return new OperationCancelToken(Database.Configuration.Core.DatabaseOperationTimeout.AsTimeSpan, Database.DatabaseShutdown);
        }

        protected OperationCancelToken CreateOperationToken()
        {
            return new OperationCancelToken(Database.DatabaseShutdown);
        }

        protected void AddPagingPerformanceHint(PagingOperationType operation, string action, HttpContext httpContext, int numberOfResults, int pageSize, TimeSpan duration)
        {
            if (numberOfResults <= Database.Configuration.PerformanceHints.MaxNumberOfResults)
                return;

            Database.NotificationCenter.Paging.Add(operation, action, httpContext.Request.QueryString.Value, numberOfResults, pageSize, duration);
        }
    }
}
