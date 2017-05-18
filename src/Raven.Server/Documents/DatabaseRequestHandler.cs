using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using System.Linq;
using Raven.Client.Documents.Transformers;
using Raven.Server.NotificationCenter.Notifications.Details;
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

            if (context.HttpContext.Request.Headers.TryGetValue("Topology-Etag", out var topologyEtag) &&
                topologyEtag.Count == 1 &&
                Database.RachisLogIndexNotifications.IsMatch(topologyEtag[0]))
            {
                context.HttpContext.Response.Headers["Refresh-Topology"] = "true";
            }
        }

        protected OperationCancelToken CreateTimeLimitedOperationToken()
        {
            return new OperationCancelToken(Database.Configuration.Core.DatabaseOperationTimeout.AsTimeSpan, Database.DatabaseShutdown);
        }

        protected OperationCancelToken CreateOperationToken()
        {
            return new OperationCancelToken(Database.DatabaseShutdown);
        }

        protected BlittableJsonReaderObject GetTransformerParameters(JsonOperationContext context)
        {
            var transformerParameters = new DynamicJsonValue();

            foreach (var kvp in HttpContext.Request.Query.Where(x => x.Key.StartsWith(TransformerParameter.Prefix)))
                transformerParameters[kvp.Key.Substring(TransformerParameter.Prefix.Length)] = kvp.Value[0];

            return context.ReadObject(transformerParameters, "transformer/parameters");
        }

        protected void AddPagingPerformanceHint(PagingOperationType operation, string action, int numberOfResults, int pageSize)
        {
            if (numberOfResults <= Database.Configuration.PerformanceHints.MaxNumberOfResults)
                return;

            Database.NotificationCenter.Paging.Add(operation, action, numberOfResults, pageSize);
        }
    }
}