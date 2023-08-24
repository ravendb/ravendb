using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System.Processors.Databases;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System.Processors.Studio;

internal sealed class StudioDatabasesHandlerForGetDatabases : AbstractDatabasesHandlerProcessorForAllowedDatabases<StudioDatabasesHandlerForGetDatabases.StudioDatabasesInfo>
{
    public StudioDatabasesHandlerForGetDatabases([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var name = GetName();
            var items = await GetAllowedDatabaseRecordsAsync(name, context, GetStart(), GetPageSize())
                .ToListAsync();

            if (items.Count == 0 && name != null)
            {
                await RequestHandler.NoContent(HttpStatusCode.NotFound);
                return;
            }

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WriteArray(context, nameof(StudioDatabasesInfo.Databases), items, (w, _, record) =>
                {
                    var databaseName = record.DatabaseName;

                    WriteStudioDatabaseInfo(databaseName, record, context, w);
                });

                writer.WriteEndObject();
            }
        }
    }

    protected override ValueTask<RavenCommand<StudioDatabasesInfo>> CreateCommandForNodeAsync(string nodeTag, JsonOperationContext context) => throw new NotSupportedException();

    private void WriteStudioDatabaseInfo(string databaseName, RawDatabaseRecord record, TransactionOperationContext context, AbstractBlittableJsonTextWriter writer)
    {
        var studioEnvironment = GetStudioEnvironment(record);
        var info = StudioDatabaseInfo.From(record, studioEnvironment, context, ServerStore, HttpContext);
        var djv = info.ToJson();

        context.Write(writer, djv);
    }

    internal sealed class StudioDatabasesInfo
    {
        public List<StudioDatabaseInfo> Databases { get; set; }
    }

    internal sealed class StudioDatabaseInfo : IDynamicJson
    {
        private StudioDatabaseInfo()
        {
        }

        public string Name { get; set; }

        public bool IsSharded { get; set; }

        public bool IsDisabled { get; set; }

        public bool IsEncrypted { get; set; }

        public int? IndexesCount { get; set; }

        public StudioConfiguration.StudioEnvironment StudioEnvironment { get; set; }

        public bool HasRevisionsConfiguration { get; set; }

        public bool HasExpirationConfiguration { get; set; }

        public bool HasRefreshConfiguration { get; set; }
        
        public bool HasDataArchivalConfiguration { get; set; }

        public Dictionary<string, DeletionInProgressStatus> DeletionInProgress { get; set; }

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
                [nameof(IsEncrypted)] = IsEncrypted,
                [nameof(IndexesCount)] = IndexesCount,
                [nameof(StudioEnvironment)] = StudioEnvironment,
                [nameof(HasRevisionsConfiguration)] = HasRevisionsConfiguration,
                [nameof(HasExpirationConfiguration)] = HasExpirationConfiguration,
                [nameof(HasRefreshConfiguration)] = HasRefreshConfiguration,
                [nameof(HasDataArchivalConfiguration)] = HasDataArchivalConfiguration,
                [nameof(DeletionInProgress)] = DynamicJsonValue.Convert(DeletionInProgress),
                [nameof(LockMode)] = LockMode,
                [nameof(NodesTopology)] = NodesTopology?.ToJson(),
                [nameof(Sharding)] = Sharding?.ToJson(),
            };
        }

        public static StudioDatabaseInfo From([NotNull] RawDatabaseRecord record, StudioConfiguration.StudioEnvironment studioEnvironment, [NotNull] TransactionOperationContext context, [NotNull] ServerStore serverStore, [NotNull] HttpContext httpContext)
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
                IsSharded = record.IsSharded,
                StudioEnvironment = studioEnvironment,
                HasExpirationConfiguration = record.ExpirationConfiguration != null,
                HasRefreshConfiguration = record.RefreshConfiguration != null,
                HasDataArchivalConfiguration = record.DataArchivalConfiguration != null,
                HasRevisionsConfiguration = record.RevisionsConfiguration != null,
                DeletionInProgress = record.DeletionInProgress,
                IndexesCount = record.CountOfIndexes
            };

            if (record.IsSharded)
            {
                result.Sharding = ShardingInfo.From(record, context, serverStore, httpContext);
            }
            else
            {
                var nodesTopology = new NodesTopology();
                DatabasesHandlerProcessorForGet.FillNodesTopology(ref nodesTopology, record.Topology, record, context, serverStore, httpContext);
                result.NodesTopology = nodesTopology;
            }

            return result;
        }

        public sealed class ShardingInfo : IDynamicJson
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

            public sealed class OrchestratorInfo : IDynamicJson
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
