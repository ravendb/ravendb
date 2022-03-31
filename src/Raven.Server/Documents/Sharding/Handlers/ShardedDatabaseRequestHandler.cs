using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx;
using Raven.Client;
using Raven.Client.Extensions;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Configuration;
using Raven.Server.Web;
using Sparrow.Logging;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public partial class ShardedDatabaseRequestHandler : RequestHandler
    {
        public ShardedDatabaseContext DatabaseContext;
        public TransactionContextPool ContextPool;
        protected Logger Logger;

        public HttpMethod Method;
        public string RelativeShardUrl;
        public string BaseShardUrl;

        public ShardExecutor ShardExecutor => DatabaseContext.ShardExecutor;

        public ShardedContinuationTokensHandler ContinuationTokens;

        public ShardedDatabaseRequestHandler()
        {
            ContinuationTokens = new ShardedContinuationTokensHandler(this);
        }

        public override void Init(RequestHandlerContext context)
        {
            base.Init(context);
            DatabaseContext = context.DatabaseContext;
            //TODO - sharding: We probably want to put it in the ShardedDatabaseContext, not use the server one 
            ContextPool = context.RavenServer.ServerStore.ContextPool;
            Logger = LoggingSource.Instance.GetLogger(DatabaseContext.DatabaseName, GetType().FullName);

            var request = HttpContext.Request;
            var url = request.Path.Value;
            var relativeIndex = url.IndexOf('/', 11); //start after "/databases/" and skip the database name

            BaseShardUrl = url.Substring(relativeIndex);
            RelativeShardUrl = BaseShardUrl + request.QueryString;
            Method = new HttpMethod(request.Method);

            context.HttpContext.Response.OnStarting(() => CheckForChanges(context));
        }

        public Task CheckForChanges(RequestHandlerContext context)
        {
            var topologyEtag = GetLongFromHeaders(Constants.Headers.TopologyEtag);
            if (topologyEtag.HasValue && DatabaseContext.HasTopologyChanged(topologyEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshTopology] = "true";

            var clientConfigurationEtag = GetLongFromHeaders(Constants.Headers.ClientConfigurationEtag);
            if (clientConfigurationEtag.HasValue && ClientConfigurationHelper.HasClientConfigurationChanged(DatabaseContext.DatabaseRecord.Client, ServerStore, clientConfigurationEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshClientConfiguration] = "true";

            return Task.CompletedTask;
        }

        public override async Task WaitForIndexToBeAppliedAsync(TransactionOperationContext context, long index)
        {
            await ServerStore.Cluster.WaitForIndexNotification(index);
            using (var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(HttpContext.RequestAborted))
            {
                cancellationTokenSource.CancelAfter(TimeSpan.FromMinutes(5));
                await DatabaseContext.RachisLogIndexNotifications.WaitForIndexNotification(index, cancellationTokenSource.Token);

                var dbs = ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(DatabaseContext.DatabaseName).ToList();
                var tasks = new List<Task>();

                foreach (var task in dbs)
                {
                    var notificationTask = task.ContinueWith(async t =>
                    {
                        var db = await t;
                        await db.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout);
                    });
                    tasks.Add(notificationTask);
                }
                
                await tasks.WhenAll().WaitAndThrowOnTimeout(TimeSpan.FromMinutes(5));
            }
        }
    }
}
