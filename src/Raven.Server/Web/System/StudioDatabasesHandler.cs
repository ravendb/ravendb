using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System.Processors.Databases;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System;

public class StudioDatabasesHandler : RequestHandler
{
    [RavenAction("/studio-tasks/databases", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task Databases()
    {
        var databaseName = GetStringQueryString("name", required: false);
        var namesOnly = GetBoolValueQueryString("namesOnly", required: false) ?? false;

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            IEnumerable<RawDatabaseRecord> items;
            if (string.IsNullOrEmpty(databaseName) == false)
            {
                var item = ServerStore.Cluster.ReadRawDatabaseRecord(context, databaseName);
                if (item == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                items = new[] { item };
            }
            else
            {
                items = ServerStore.Cluster.GetAllRawDatabases(context, GetStart(), GetPageSize());
            }

            var allowedDbs = await GetAllowedDbsAsync(null, requireAdmin: false, requireWrite: false);

            if (allowedDbs.HasAccess == false)
                return;

            if (allowedDbs.AuthorizedDatabases != null)
                items = items.Where(item => allowedDbs.AuthorizedDatabases.ContainsKey(item.DatabaseName));

            await using var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream());

            writer.WriteStartObject();

            writer.WriteArray(context, "Databases", items, (w, c, i) =>
            {
                var info = StudioDatabaseInfo.From(i, context, ServerStore, HttpContext);
                var djv = info.ToJson();

                c.Write(w, djv);
            });

            writer.WriteEndObject();
        }
    }

    private class StudioDatabaseInfo : IDynamicJson
    {
        private StudioDatabaseInfo()
        {
        }

        public string Name { get; set; }

        public bool IsSharded { get; set; }

        public bool IsDisabled { get; set; }

        public bool IsEncrypted { get; set; }

        public DatabaseLockMode LockMode { get; set; }

        public NodesTopology NodesTopology { get; set; }

        public ShardingInfo Sharding { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(IsSharded)] = IsSharded,
                [nameof(IsDisabled)] = IsDisabled,
                [nameof(LockMode)] = LockMode,
                [nameof(NodesTopology)] = NodesTopology?.ToJson(),
                [nameof(Sharding)] = Sharding?.ToJson()
            };
        }

        public static StudioDatabaseInfo From([NotNull] RawDatabaseRecord record, [NotNull] TransactionOperationContext context, [NotNull] ServerStore serverStore, [NotNull] HttpContext httpContext)
        {
            if (record == null)
                throw new ArgumentNullException(nameof(record));
            if (context == null)
                throw new ArgumentNullException(nameof(context));
            if (serverStore == null)
                throw new ArgumentNullException(nameof(serverStore));
            if (httpContext == null)
                throw new ArgumentNullException(nameof(httpContext));

            var result = new StudioDatabaseInfo
            {
                Name = record.DatabaseName,
                IsDisabled = record.IsDisabled,
                LockMode = record.LockMode,
                IsEncrypted = record.IsEncrypted,
                IsSharded = record.IsSharded
            };

            var nodesTopology = new NodesTopology();
            DatabasesHandlerProcessorForGet.FillNodesTopology(ref nodesTopology, record.Topology, record, context, serverStore, httpContext);
            result.NodesTopology = nodesTopology;

            if (record.IsSharded)
                result.Sharding = ShardingInfo.From(record, context, serverStore, httpContext);

            return result;
        }

        public class ShardingInfo : IDynamicJson
        {
            private ShardingInfo()
            {
            }

            public OrchestratorInfo Orchestrator { get; set; }

            public Dictionary<int, NodesTopology> Shards { get; set; }

            public DynamicJsonValue ToJson()
            {
                var shards = new DynamicJsonValue();
                foreach (var kvp in Shards)
                    shards[kvp.Key.ToString()] = kvp.Value?.ToJson();

                var result = new DynamicJsonValue
                {
                    [nameof(Orchestrator)] = Orchestrator.ToJson(),
                    [nameof(Shards)] = shards
                };

                return result;
            }

            public static ShardingInfo From([NotNull] RawDatabaseRecord record, [NotNull] TransactionOperationContext context, [NotNull] ServerStore serverStore, [NotNull] HttpContext httpContext)
            {
                if (record == null)
                    throw new ArgumentNullException(nameof(record));
                if (context == null)
                    throw new ArgumentNullException(nameof(context));
                if (serverStore == null)
                    throw new ArgumentNullException(nameof(serverStore));
                if (httpContext == null)
                    throw new ArgumentNullException(nameof(httpContext));

                var orchestrator = new OrchestratorInfo();

                var orchestratorTopology = new NodesTopology();
                DatabasesHandlerProcessorForGet.FillNodesTopology(ref orchestratorTopology, record.Sharding.Orchestrator.Topology, record, context, serverStore, httpContext);
                orchestrator.NodesTopology = orchestratorTopology;

                var result = new ShardingInfo
                {
                    Orchestrator = orchestrator,
                    Shards = new Dictionary<int, NodesTopology>()
                };

                foreach (var kvp in record.Sharding.Shards)
                {
                    var shardTopology = new NodesTopology();
                    DatabasesHandlerProcessorForGet.FillNodesTopology(ref shardTopology, kvp.Value, record, context, serverStore, httpContext);

                    result.Shards[kvp.Key] = shardTopology;
                }

                return result;
            }

            public class OrchestratorInfo : IDynamicJson
            {
                public NodesTopology NodesTopology { get; set; }

                public DynamicJsonValue ToJson()
                {
                    return new DynamicJsonValue
                    {
                        [nameof(NodesTopology)] = NodesTopology?.ToJson()
                    };
                }
            }
        }
    }
}
