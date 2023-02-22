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
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Raven.Server.Web.System.Processors.Databases;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Web.System.Processors.Studio;

internal class StudioDatabasesHandlerForGetDatabasesState : AbstractServerHandlerProxyReadProcessor<StudioDatabasesHandlerForGetDatabasesState.StudioDatabasesState>
{
    private static readonly Logger Logger = LoggingSource.Instance.GetLogger<DatabasesHandler>("Server");

    public StudioDatabasesHandlerForGetDatabasesState([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var name = GetName();
            var items = await DatabasesHandlerProcessorForGet.GetDatabaseRecordsAsync(name, ServerStore, RequestHandler, context, GetStart(), GetPageSize())
                .ToListAsync();

            if (items.Count == 0 && name != null)
            {
                await RequestHandler.NoContent(HttpStatusCode.NotFound);
                return;
            }

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WriteArray(context, nameof(StudioDatabasesState.Databases), items.SelectMany(x => x.AsShardsOrNormal()), (w, _, record) =>
                {
                    var databaseName = record.DatabaseName;

                    WriteStudioDatabaseState(databaseName, record, context, w);
                });

                writer.WriteEndObject();
            }
        }
    }

    protected override RavenCommand<StudioDatabasesState> CreateCommandForNode(string nodeTag)
    {
        var name = GetName();
        if (name != null)
            return new GetStudioDatabasesStateCommand(name, nodeTag);

        var start = GetStart();
        var pageSize = GetPageSize();

        return new GetStudioDatabasesStateCommand(start, pageSize, nodeTag);
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<StudioDatabasesState> command, JsonOperationContext context, OperationCancelToken token)
    {
        return RequestHandler.ServerStore.ClusterRequestExecutor.ExecuteAsync(command, context, token: token.Token);
    }

    private string GetName() => RequestHandler.GetStringQueryString("name", required: false);

    private int GetStart() => RequestHandler.GetStart();

    private int GetPageSize() => RequestHandler.GetPageSize();

    private void WriteStudioDatabaseState(string databaseName, RawDatabaseRecord record, TransactionOperationContext context, AbstractBlittableJsonTextWriter writer)
    {
        try
        {
            var online = ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(databaseName, out Task<DocumentDatabase> dbTask) && dbTask is { IsCompleted: true };

            // Check for exceptions
            if (dbTask is { IsFaulted: true })
            {
                var exception = dbTask.Exception.ExtractSingleInnerException();
                WriteFaultedDatabaseState(databaseName, exception, context, writer);
                return;
            }

            var database = online ? dbTask.Result : null;
            var indexingStatus = DatabasesHandlerProcessorForGet.GetIndexingStatus(record, database);
            var upTime = DatabasesHandlerProcessorForGet.GetUpTime(database);
            var backupInfo = DatabasesHandlerProcessorForGet.GetBackupInfo(databaseName, record, database, ServerStore, context);
            var state = StudioDatabaseState.From(databaseName, database, backupInfo, upTime, indexingStatus);

            context.Write(writer, state.ToJson());
        }
        catch (Exception e)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Failed to get database info for: {databaseName}", e);

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
            [nameof(DatabaseInfo.LoadError)] = exception.Message
        };

        context.Write(writer, doc);
    }

    private class GetStudioDatabasesStateCommand : RavenCommand<StudioDatabasesState>
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
            url = $"{node.Url}/databases?";

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

    public class StudioDatabasesState
    {
        public List<StudioDatabaseState> Databases { get; set; }
    }

    public class StudioDatabaseState : DatabaseState
    {
        public static StudioDatabaseState From(string databaseName, DocumentDatabase database, BackupInfo backupInfo, TimeSpan? upTime, IndexRunningStatus? indexingStatus)
        {
            var size = database?.GetSizeOnDisk() ?? (new Size(0), new Size(0));

            return new StudioDatabaseState
            {
                Name = databaseName,
                TotalSize = size.Data,
                TempBuffersSize = size.TempBuffers,
                UpTime = upTime,
                BackupInfo = backupInfo,
                Alerts = database?.NotificationCenter.GetAlertCount() ?? 0,
                PerformanceHints = database?.NotificationCenter.GetPerformanceHintCount() ?? 0,
                LoadError = null,
                IndexingErrors = database?.IndexStore?.GetIndexes()?.Sum(index => index.GetErrorCount()) ?? 0,
                DocumentsCount = database?.DocumentsStorage.GetNumberOfDocuments() ?? 0,
                IndexingStatus = indexingStatus ?? IndexRunningStatus.Running,
            };
        }
    }
}
