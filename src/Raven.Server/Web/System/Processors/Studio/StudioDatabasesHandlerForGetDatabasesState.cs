using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.Json;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System.Processors.Databases;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Web.System.Processors.Studio;

internal sealed class StudioDatabasesHandlerForGetDatabasesState : AbstractDatabasesHandlerProcessorForAllowedDatabases<StudioDatabasesHandlerForGetDatabasesState.StudioDatabasesState>
{
    private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<StudioDatabasesHandlerForGetDatabasesState>();

    public StudioDatabasesHandlerForGetDatabasesState([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        using (ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
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

                writer.WriteArray(context, nameof(StudioDatabasesState.Orchestrators), GetOrchestrators(items, RequestHandler.ServerStore.NodeTag), (w, _, record) =>
                {
                    var databaseName = record.DatabaseName;

                    WriteStudioOrchestratorState(databaseName, context, w);
                });

                writer.WriteComma();

                writer.WriteArray(context, nameof(StudioDatabasesState.Databases), items.SelectMany(x => x.AsShardsOrNormal(RequestHandler.ServerStore.NodeTag)), (w, _, record) =>
                {
                    var databaseName = record.DatabaseName;

                    WriteStudioDatabaseState(databaseName, record, context, w);
                });

                writer.WriteEndObject();
            }
        }
    }

    protected override ValueTask<RavenCommand<StudioDatabasesState>> CreateCommandForNodeAsync(string nodeTag, JsonOperationContext context)
    {
        var name = GetName();
        if (name != null)
            return ValueTask.FromResult<RavenCommand<StudioDatabasesState>>(new GetStudioDatabasesStateCommand(name, nodeTag));

        var start = GetStart();
        var pageSize = GetPageSize();

        return ValueTask.FromResult<RavenCommand<StudioDatabasesState>>(new GetStudioDatabasesStateCommand(start, pageSize, nodeTag));
    }

    private static IEnumerable<RawDatabaseRecord> GetOrchestrators([NotNull] List<RawDatabaseRecord> records, [NotNull] string nodeTag)
    {
        if (records == null)
            throw new ArgumentNullException(nameof(records));
        if (string.IsNullOrEmpty(nodeTag))
            throw new ArgumentException("Value cannot be null or empty.", nameof(nodeTag));

        foreach (var record in records)
        {
            if (record.IsSharded == false)
                continue;

            if (record.Sharding.Orchestrator.Topology.RelevantFor(nodeTag) == false)
                continue;

            yield return record;
        }
    }

    private void WriteStudioOrchestratorState(string databaseName, JsonOperationContext context, AbstractBlittableJsonTextWriter writer)
    {
        try
        {
            var searchResult = ServerStore.DatabasesLandlord.TryGetOrCreateDatabase(databaseName);
            var databaseContext = searchResult.DatabaseContext;

            var state = StudioOrchestratorState.From(databaseName, databaseContext);

            context.Write(writer, state.ToJson());
        }
        catch (Exception e)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Failed to get orchestrator info for '{databaseName}' database.", e);

            WriteFaultedOrchestratorState(databaseName, e, context, writer);
        }
    }

    private void WriteStudioDatabaseState(string databaseName, RawDatabaseRecord record, ClusterOperationContext context, AbstractBlittableJsonTextWriter writer)
    {
        try
        {
            ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(databaseName, out Task<DocumentDatabase> dbTask);
            StudioDatabaseStatus status = StudioDatabaseStatus.None;
            if (dbTask != null)
            {
                if (dbTask.IsCompleted)
                    status = StudioDatabaseStatus.Online;
                else if (dbTask.IsFaulted)
                    status = StudioDatabaseStatus.Error;
                else
                    status = StudioDatabaseStatus.Loading;
            }

            // Check for exceptions
            if (status == StudioDatabaseStatus.Error)
            {
                var exception = dbTask.Exception.ExtractSingleInnerException();
                WriteFaultedDatabaseState(databaseName, exception, context, writer);
                return;
            }

            var database = status == StudioDatabaseStatus.Online ? dbTask.Result : null;
            var indexingStatus = GetIndexingStatus(record, database);
            var upTime = GetUpTime(database);
            var backupInfo = GetBackupInfo(databaseName, record, database, ServerStore, context);
            var state = StudioDatabaseState.From(databaseName, status, database, ServerStore.DatabaseInfoCache, backupInfo, upTime, indexingStatus);

            context.Write(writer, state.ToJson());
        }
        catch (Exception e)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Failed to get database info for '{databaseName}' database.", e);

            WriteFaultedDatabaseState(databaseName, e, context, writer);
        }
    }

    private static void WriteFaultedDatabaseState(string databaseName,
        Exception exception,
        JsonOperationContext context,
        AbstractBlittableJsonTextWriter writer)
    {
        var doc = new DynamicJsonValue
        {
            [nameof(DatabaseInfo.Name)] = databaseName,
            [nameof(DatabaseInfo.LoadError)] = exception.Message,
            [nameof(StudioDatabaseStatus)] = StudioDatabaseStatus.Error
        };

        context.Write(writer, doc);
    }

    private static void WriteFaultedOrchestratorState(string databaseName,
        Exception exception,
        JsonOperationContext context,
        AbstractBlittableJsonTextWriter writer)
    {
        var doc = new DynamicJsonValue
        {
            [nameof(StudioOrchestratorState.Name)] = databaseName,
            [nameof(StudioOrchestratorState.LoadError)] = exception.Message
        };

        context.Write(writer, doc);
    }

    private sealed class GetStudioDatabasesStateCommand : RavenCommand<StudioDatabasesState>
    {
        private readonly int? _start;
        private readonly int? _pageSize;
        private readonly string _name;

        public GetStudioDatabasesStateCommand([NotNull] string name, string nodeTag)
        {
            _name = name ?? throw new ArgumentNullException(nameof(name));
            SelectedNodeTag = nodeTag;
        }

        public GetStudioDatabasesStateCommand(int start, int pageSize, string nodeTag)
        {
            _start = start;
            _pageSize = pageSize;
            SelectedNodeTag = nodeTag;
        }

        public override bool IsReadRequest => false;

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/studio-tasks/databases/state?";

            if (_name != null)
                url += $"name={Uri.EscapeDataString(_name)}";

            if (_start > 0)
                url += $"&start={_start}";

            if (_pageSize != int.MaxValue)
                url += $"&pageSize={_pageSize}";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                return;

            Result = JsonDeserializationServer.StudioDatabasesState(response);
        }
    }

    internal sealed class StudioDatabasesState
    {
        public List<StudioOrchestratorState> Orchestrators { get; set; }

        public List<StudioDatabaseState> Databases { get; set; }
    }

    internal sealed class StudioOrchestratorState : IDynamicJson
    {
        public string Name { get; set; }
        public long Alerts { get; set; }
        public long PerformanceHints { get; set; }
        public string LoadError { get; set; }

        public static StudioOrchestratorState From([NotNull] string databaseName, [NotNull] ShardedDatabaseContext database)
        {
            if (databaseName == null)
                throw new ArgumentNullException(nameof(databaseName));
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            return new StudioOrchestratorState
            {
                Name = database.DatabaseName ?? databaseName,
                PerformanceHints = database.NotificationCenter.GetPerformanceHintCount(),
                Alerts = database.NotificationCenter.GetAlertCount()
            };
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Name)] = Name,
                [nameof(Alerts)] = Alerts,
                [nameof(PerformanceHints)] = PerformanceHints
            };
        }
    }

    internal enum StudioDatabaseStatus
    {
        None,
        Online,
        Loading,
        Error
    }

    internal sealed class StudioDatabaseState : DatabaseState
    {
        public StudioDatabaseStatus DatabaseStatus { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var djv = base.ToJson();
            djv[nameof(DatabaseStatus)] = DatabaseStatus;

            return djv;
        }

        public static StudioDatabaseState From(string databaseName, StudioDatabaseStatus databaseStatus, DocumentDatabase database, DatabaseInfoCache databaseInfoCache, BackupInfo backupInfo, TimeSpan? upTime, IndexRunningStatus? indexingStatus)
        {
            Size totalSize = null;
            Size tempBuffersSize = null;
            long alerts = 0;
            long performanceHints = 0;
            long indexingErrors = 0;
            long documentsCount = 0;
            if (database != null)
            {
                (Size data, Size tempBuffers) = database.GetSizeOnDisk();
                totalSize = data;
                tempBuffersSize = tempBuffers;

                alerts = database.NotificationCenter.GetAlertCount();
                performanceHints = database.NotificationCenter.GetPerformanceHintCount();
                indexingErrors = database.IndexStore?.GetIndexes()?.Sum(index => index.GetErrorCount()) ?? 0;
                documentsCount = database.DocumentsStorage.GetNumberOfDocuments();
            }
            else if (databaseInfoCache.TryGet(databaseName, json =>
                     {
                         if (json == null)
                             return;

                         json.TryGet(nameof(Alerts), out alerts);
                         json.TryGet(nameof(PerformanceHints), out performanceHints);
                         json.TryGet(nameof(IndexingErrors), out indexingErrors);
                         json.TryGet(nameof(DocumentsCount), out documentsCount);

                         totalSize = GetSize(json, nameof(TotalSize));
                         tempBuffersSize = GetSize(json, nameof(TempBuffersSize));
                     }))
            {
                // nothing to do here
            }

            return new StudioDatabaseState
            {
                Name = databaseName,
                TotalSize = totalSize,
                TempBuffersSize = tempBuffersSize,
                UpTime = upTime,
                BackupInfo = backupInfo,
                Alerts = alerts,
                PerformanceHints = performanceHints,
                LoadError = null,
                IndexingErrors = indexingErrors,
                DocumentsCount = documentsCount,
                IndexingStatus = indexingStatus ?? IndexRunningStatus.Running,
                DatabaseStatus = databaseStatus
            };

            static Size GetSize(BlittableJsonReaderObject json, string propertyName)
            {
                if (json.TryGet(propertyName, out BlittableJsonReaderObject sizeJson) && sizeJson != null)
                {
                    if (sizeJson.TryGet(nameof(Size.SizeInBytes), out long sizeInBytes))
                        return new Size(sizeInBytes);
                }

                return null;
            }
        }
    }
}
