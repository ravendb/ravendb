using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Handlers;
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

        [RavenAction("/info/remote-task/topology", "GET", AuthorizationStatus.RestrictedAccess)]
        public Task GetRemoteTaskTopology()
        {
            var database = GetStringQueryString("database");
            var databaseGroupId = GetStringQueryString("groupId");
            var remoteTask = GetStringQueryString("remote-task");
            PullReplicationHandler.Authenticate(HttpContext, ServerStore, database, remoteTask);

            List<string> nodes;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var pullReplication = ServerStore.Cluster.ReadPullReplicationDefinition(database, remoteTask, context);
                var topology = ServerStore.Cluster.ReadDatabaseTopology(context, database);
                nodes = GetResponsibleNodes(topology, databaseGroupId, pullReplication);
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var output = new DynamicJsonArray();
                var clusterTopology = ServerStore.GetClusterTopology();
                foreach (var node in nodes)
                {
                    output.Add(clusterTopology.GetUrlFromTag(node));
                }
                context.Write(writer, new DynamicJsonValue
                {
                    ["Results"] = output
                });
            }
            return Task.CompletedTask;
        }

        private List<string> GetResponsibleNodes(DatabaseTopology topology, string databaseGroupId, PullReplicationDefinition pullReplication)
        {
            var list = new List<string>();
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
