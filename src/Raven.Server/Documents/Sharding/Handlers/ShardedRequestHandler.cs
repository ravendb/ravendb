using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public partial class ShardedRequestHandler : RequestHandler
    {
        public ShardedContext ShardedContext;
        public TransactionContextPool ContextPool;
        protected Logger Logger;

        public HttpMethod Method;
        public string RelativeShardUrl;
        public string BaseShardUrl;

        public ShardExecutor ShardExecutor => ShardedContext.ShardExecutor;

        public ShardedContinuationTokensHandler ContinuationTokens;

        public ShardedRequestHandler()
        {
            ContinuationTokens = new ShardedContinuationTokensHandler(this);
        }

        public override void Init(RequestHandlerContext context)
        {
            base.Init(context);
            ShardedContext = context.ShardedContext;
            //TODO - sharding: We probably want to put it in the ShardedContext, not use the server one 
            ContextPool = context.RavenServer.ServerStore.ContextPool;
            Logger = LoggingSource.Instance.GetLogger(ShardedContext.DatabaseName, GetType().FullName);
            var topologyEtag = GetLongFromHeaders(Constants.Headers.TopologyEtag);
            if (topologyEtag.HasValue && ShardedContext.HasTopologyChanged(topologyEtag.Value))
            {
                context.HttpContext.Response.Headers[Constants.Headers.RefreshTopology] = "true";
            }

            var clientConfigurationEtag = GetLongFromHeaders(Constants.Headers.ClientConfigurationEtag);
            if (clientConfigurationEtag.HasValue && ShardedContext.HasClientConfigurationChanged(clientConfigurationEtag.Value))
                context.HttpContext.Response.Headers[Constants.Headers.RefreshClientConfiguration] = "true";

            var request = HttpContext.Request;
            var url = request.Path.Value;
            var relativeIndex = url.IndexOf('/', 11); //start after "/databases/" and skip the database name

            BaseShardUrl = url.Substring(relativeIndex);
            RelativeShardUrl = BaseShardUrl + request.QueryString;
            Method = new HttpMethod(request.Method);
        }

        public override async Task WaitForIndexToBeApplied(TransactionOperationContext context, long index)
        {
            var dbs = ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(ShardedContext.DatabaseName).ToList();
            if (dbs.Count == 0)
            {
                await ServerStore.Cluster.WaitForIndexNotification(index);
            }
            else
            {
                var tasks = new List<Task>();
                foreach (var task in dbs)
                {
                    var db = await task;
                    tasks.Add(db.RachisLogIndexNotifications.WaitForIndexNotification(index, ServerStore.Engine.OperationTimeout));
                }
                await Task.WhenAll(tasks);
            }
        }

        protected async Task WaitForExecutionOfRaftCommands(JsonOperationContext context, List<long> raftIndexIds)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Should be modified after we migrate to ShardedExecutor");

            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(ServerStore.ServerShutdown))
            {
                var timeToWait = ServerStore.Configuration.Cluster.OperationTimeout.GetValue(TimeUnit.Milliseconds) * raftIndexIds.Count;
                cts.CancelAfter(TimeSpan.FromMilliseconds(timeToWait));

                var requestExecutors = ShardedContext.RequestExecutors;
                var waitingTasks = new Task[requestExecutors.Length];

                var waitForDatabaseCommands = new WaitForRaftCommands(raftIndexIds);
                for (var index = 0; index < requestExecutors.Length; index++)
                {
                    waitingTasks[index] = requestExecutors[index].ExecuteAsync(waitForDatabaseCommands, context, token: cts.Token);
                }
                await Task.WhenAll(waitingTasks);
            }
        }
    }
}
