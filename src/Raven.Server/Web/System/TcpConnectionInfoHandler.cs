using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Extensions;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class TcpConnectionInfoHandler : RequestHandler
    {
        [RavenAction("/info/tcp", "GET", AuthorizationStatus.ValidUser)]
        public Task Get()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var output = Server.ServerStore.GetTcpInfoAndCertificates(HttpContext.Request.GetClientRequestedNodeUrl());
                context.Write(writer, output);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/info/tcp/pull", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public async Task GetPullReplicationInfo()
        {
            var databaseName = GetStringQueryString("databaseName");
            var databaseGroupId = GetStringQueryString("databaseGroupId");
            var definitionName = GetStringQueryString("definitionName");

            var nodes = GetResponsibleNode(databaseName, databaseGroupId, definitionName);

            // TODO: cache the tcp info for each node?
            DynamicJsonArray output = new DynamicJsonArray();
            var clusterTopology = ServerStore.GetClusterTopology();
            var tasks = new List<Task<TcpConnectionInfo>>();
            foreach (var node in nodes)
            {
                if (node == ServerStore.NodeTag)
                {
                    output.Add(Server.ServerStore.GetTcpInfoAndCertificates(HttpContext.Request.GetClientRequestedNodeUrl()));
                }
                else
                {
                    var nodeUrl = clusterTopology.GetUrlFromTag(node);
                    tasks.Add(GetNodeTcpInfo(nodeUrl, databaseName));
                }
            }

            await Task.WhenAll(tasks);
            foreach (var task in tasks)
            {
                output.Add(task.Result.ToJson());
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    ["Results"] = output
                });
            }
        }

        private async Task<TcpConnectionInfo> GetNodeTcpInfo(string nodeUrl, string databaseName)
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (var requestExecutor = ClusterRequestExecutor.CreateForSingleNode(nodeUrl, Server.Certificate.Certificate))
            {
                requestExecutor.DefaultTimeout = ServerStore.Engine.OperationTimeout;
                var infoCmd = new GetTcpInfoCommand("database-group-tcp", databaseName);
                await requestExecutor.ExecuteAsync(infoCmd, ctx);
                return infoCmd.Result;
            }
        }

        private List<string> GetResponsibleNode(string databaseName, string databaseGroupId, string definitionName)
        {
            var list = new List<string>();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var pullReplication = ServerStore.Cluster.ReadPullReplicationDefinition(databaseName, definitionName, context);
                var cert = HttpContext.Connection.ClientCertificate;
                pullReplication.Validate(cert?.Thumbprint);

                var topology = ServerStore.Cluster.ReadDatabaseTopology(context, databaseName);
                // we distribute connections to have load balancing when many edges are connected.
                // this is the central cluster, so we make the decision which node will do the pull replication only once and only here,
                // for that we create a dummy IDatabaseTask.
                var mentorNodeTask = new PullNodeTask
                {
                    Mentor = pullReplication.MentorNode,
                    DatabaseGroupId = databaseGroupId
                };

                while (topology.Members.Count > 0)
                {
                    var next = topology.WhoseTaskIsIt(ServerStore.CurrentRachisState, mentorNodeTask, null);
                    list.Add(next);
                    topology.Members.Remove(next);
                }

                return list;
            }
        }

        private class PullNodeTask : IDatabaseTask
        {
            public string Mentor;
            public string DatabaseGroupId;

            public ulong GetTaskKey()
            {
                return Hashing.XXHash64.Calculate(DatabaseGroupId, Encodings.Utf8);
            }

            public string GetMentorNode()
            {
                return Mentor;
            }

            public string GetDefaultTaskName()
            {
                throw new NotImplementedException();
            }

            public string GetTaskName()
            {
                throw new NotImplementedException();
            }
        }
    }
}
