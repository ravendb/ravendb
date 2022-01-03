using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx;
using Raven.Client;
using Raven.Server.Documents.ShardedHandlers.ContinuationTokens;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Sharding
{
    public class ShardedRequestHandler : RequestHandler
    {
        public ShardedContext ShardedContext;
        public TransactionContextPool ContextPool;
        protected Logger Logger;

        public HttpMethod Method;
        public string RelativeShardUrl;
        public string BaseShardUrl;

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

        protected internal ShardedPagingContinuation GetOrCreateContinuationToken(JsonOperationContext context)
        {
            var qToken = GetStringQueryString(ContinuationToken.ContinuationTokenQueryString, required: false);
            var token = ContinuationToken.FromBase64<ShardedPagingContinuation>(context, qToken) ??
                        new ShardedPagingContinuation(ShardedContext, GetStart(), GetPageSize());
            return token;
        }

        public ShardExecutor ShardExecutor => ShardedContext.ShardExecutor;

        public void AddHeaders<T>(ShardedBaseCommand<T> command, Headers header)
        {
            if (header == Headers.None)
                return;

            if (header.HasFlag(Headers.IfMatch))
                command.Headers["If-Match"] = GetStringFromHeaders("If-Match");

            if (header.HasFlag(Headers.IfNoneMatch))
                command.Headers["If-None-Match"] = GetStringFromHeaders("If-None-Match");
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
                await tasks.WhenAll();
            }
        }

        protected async Task WaitForExecutionOfDatabaseCommands(JsonOperationContext context, List<long> raftIndexIds)
        {
            using (var cts = CancellationTokenSource.CreateLinkedTokenSource(ServerStore.ServerShutdown))
            {
                var timeToWait = ServerStore.Configuration.Cluster.OperationTimeout.GetValue(TimeUnit.Milliseconds) * raftIndexIds.Count;
                cts.CancelAfter(TimeSpan.FromMilliseconds(timeToWait));

                var waitingTasks = new List<Task<Exception>>();
                List<Exception> exceptions = null;

                foreach (var executor in ShardedContext.RequestExecutors)
                {
                    waitingTasks.Add(ExecuteTask(executor));
                }

                while (waitingTasks.Count > 0)
                {
                    var task = await Task.WhenAny(waitingTasks);
                    waitingTasks.Remove(task);

                    if (task.Result == null)
                        continue;

                    var exception = task.Result.ExtractSingleInnerException();

                    exceptions ??= new List<Exception>();
                    exceptions.Add(exception);
                }

                if (exceptions != null)
                {
                    var allTimeouts = true;
                    foreach (var exception in exceptions)
                    {
                        if (exception is OperationCanceledException)
                            continue;

                        allTimeouts = false;
                    }

                    var aggregateException = new AggregateException(exceptions);

                    if (allTimeouts)
                        throw new TimeoutException($"Waited too long for the raft commands (numbered {string.Join(", ", raftIndexIds)}) to be executed on any of the relevant nodes to this command.",
                            aggregateException);

                    throw new InvalidDataException(
                        $"The database '{ShardedContext.DatabaseName}' was created but is not accessible, because all of the nodes on which this database was supposed to reside on, threw an exception.",
                        aggregateException);
                }

                async Task<Exception> ExecuteTask(RequestExecutor executor)
                {
                    try
                    {
                        await executor.ExecuteAsync(new WaitForCommands(raftIndexIds), context, token: cts.Token);
                        return null;
                    }
                    catch (Exception e)
                    {
                        return e;
                    }
                }
            }
        }
    }
}
