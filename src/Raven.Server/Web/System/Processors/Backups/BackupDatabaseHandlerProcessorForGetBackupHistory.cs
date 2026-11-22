using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.WebUtilities;
using Raven.Client;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.Json;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Web.System.Processors.Backups;

internal sealed class BackupDatabaseHandlerProcessorForGetBackupHistory : AbstractBackupHistoryHandlerProcessor
{
    public BackupDatabaseHandlerProcessorForGetBackupHistory([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    private const int DefaultPageSize = 50;

    private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<BackupDatabaseHandlerProcessorForGetBackupHistory>();

    protected override bool ShardNumberRequired => false;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var shardNumber = GetOptionalShardNumber();
        var query = GetQuery(shardNumber);

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            BackupHistory backupHistory;
            using (context.OpenReadTransaction())
            using (var databaseRecord = ReadDatabaseRecord(context, shardNumber))
                backupHistory = BackupHistoryStorage.GetBackupHistory(context, databaseRecord, query);

            var answeredNodeTags = GetNodeTag(required: false) == null ? await JoinOtherNodesHistoryAsync(backupHistory, query) : [];
            var sortNeedsFullBackups = query.SortBy is BackupHistorySortBy.Node or BackupHistorySortBy.Duration;

            if (answeredNodeTags.Count > 0)
            {
                using (context.OpenReadTransaction())
                    BackupHistoryStorage.CompleteGroups(context, DatabaseName, backupHistory.Groups.Where(group => group.FullBackupInHistory == false));

                if (sortNeedsFullBackups)
                    await CompleteFullBackupsFromOtherNodesAsync(backupHistory, answeredNodeTags);
            }

            backupHistory.SortAndPage(query);

            if (answeredNodeTags.Count > 0 && sortNeedsFullBackups == false)
                await CompleteFullBackupsFromOtherNodesAsync(backupHistory, answeredNodeTags);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(BackupHistory));
                context.Write(writer, backupHistory.ToJson());
                writer.WriteEndObject();
            }
        }
    }

    private BackupHistoryQuery GetQuery(int? shardNumber)
    {
        var requestedTaskId = RequestHandler.GetLongQueryString("taskId", required: false);
        var fullBackupCreatedAt = GetUtcDateTimeQueryString("fullBackupCreatedAt", required: false);

        if (fullBackupCreatedAt.HasValue && requestedTaskId.HasValue == false)
            throw new BadRequestException("'taskId' must be specified when 'fullBackupCreatedAt' is specified");

        return new BackupHistoryQuery
        {
            ShardNumber = shardNumber,
            TaskId = requestedTaskId,
            FullBackupTicks = fullBackupCreatedAt?.Ticks,
            IncludeIncrementals = RequestHandler.GetBoolValueQueryString("includeIncrementals", required: false) ?? true,
            From = GetUtcDateTimeQueryString("from", required: false),
            To = GetUtcDateTimeQueryString("to", required: false),
            TaskName = RequestHandler.GetStringQueryString("taskName", required: false),
            Triggers = RequestHandler.GetEnumValuesQueryString<BackupTrigger>("trigger"),
            BackupTypes = RequestHandler.GetEnumValuesQueryString<BackupType>("backupType"),
            Start = RequestHandler.GetStart(),
            PageSize = RequestHandler.GetPageSize(DefaultPageSize),
            SortBy = RequestHandler.GetSingleEnumValueQueryString<BackupHistorySortBy>("sortBy"),
            Descending = RequestHandler.GetBoolValueQueryString("descending", required: false) ?? true,
            GroupBy = RequestHandler.GetSingleEnumValueQueryString<BackupHistoryGroupBy>("groupBy")
        };
    }

    private async Task<List<string>> JoinOtherNodesHistoryAsync(BackupHistory backupHistory, BackupHistoryQuery query)
    {
        var answeredNodeTags = new List<string>();
        var otherNodeTags = ServerStore.GetClusterTopology().AllNodes.Keys
            .Where(nodeTag => string.Equals(nodeTag, ServerStore.NodeTag, StringComparison.OrdinalIgnoreCase) == false)
            .ToList();

        if (otherNodeTags.Count == 0)
            return answeredNodeTags;

        var pathAndQuery = GetNodeOnlyPathAndQuery(query);

        using (var token = CreateOtherNodesToken())
        {
            foreach (var (nodeTag, otherNodeHistory, error) in await Task.WhenAll(otherNodeTags.Select(nodeTag => TryGetNodeHistoryAsync(nodeTag, pathAndQuery, request: null, token.Token))))
            {
                if (otherNodeHistory == null)
                {
                    backupHistory.UnreachableNodes.Add(new BackupHistoryUnreachableNode { NodeTag = nodeTag, Error = error });
                    continue;
                }

                backupHistory.Join(otherNodeHistory);
                answeredNodeTags.Add(nodeTag);
            }
        }

        return answeredNodeTags;
    }

    private async Task CompleteFullBackupsFromOtherNodesAsync(BackupHistory backupHistory, List<string> nodeTags)
    {
        var groupsWithoutFullBackup = backupHistory.Groups.Where(group => group.FullBackupInHistory == false && group.FullBackup.CreatedAt != DateTime.MinValue).ToList();
        if (groupsWithoutFullBackup.Count == 0)
            return;

        var groupsByKey = groupsWithoutFullBackup.ToDictionary(group => (group.TaskId, group.ShardNumber, group.FullBackup.CreatedAt.Ticks));
        var request = new BackupHistory(DatabaseName)
        {
            Groups = groupsWithoutFullBackup.Select(group => new BackupGroup { TaskId = group.TaskId, ShardNumber = group.ShardNumber, FullBackup = group.FullBackup }).ToList()
        };
        var pathAndQuery = QueryHelpers.AddQueryString($"{RequestHandler.HttpContext.Request.Path}/full-backups", "database", DatabaseName);

        using (var token = CreateOtherNodesToken())
        {
            foreach (var (nodeTag, fullBackups, error) in await Task.WhenAll(nodeTags.Select(nodeTag => TryGetNodeHistoryAsync(nodeTag, pathAndQuery, request, token.Token))))
            {
                if (fullBackups == null)
                {
                    backupHistory.UnreachableNodes.Add(new BackupHistoryUnreachableNode
                    {
                        NodeTag = nodeTag,
                        Error = $"The node answered, but couldn't send the full backups of {groupsWithoutFullBackup.Count} groups: {error}"
                    });
                    continue;
                }

                foreach (var found in fullBackups.Groups)
                {
                    if (groupsByKey.TryGetValue((found.TaskId, found.ShardNumber, found.FullBackup.CreatedAt.Ticks), out var group) == false || group.FullBackupInHistory)
                        continue;

                    group.FullBackup = found.FullBackup;
                    group.FullBackupInHistory = true;
                }
            }
        }
    }

    private OperationCancelToken CreateOtherNodesToken() =>
        RequestHandler.CreateHttpRequestBoundTimeLimitedOperationToken(ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan);

    private async Task<(string NodeTag, BackupHistory History, string Error)> TryGetNodeHistoryAsync(string nodeTag, string pathAndQuery, BackupHistory request, CancellationToken token)
    {
        try
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var body = request == null ? null : context.ReadObject(request.ToJson(), "backup-history/full-backups");
                var command = new GetBackupHistoryFromNodeCommand(nodeTag, pathAndQuery, body);
                await ServerStore.ClusterRequestExecutor.ExecuteAsync(command, context, token: token);

                return (nodeTag, JsonDeserializationServer.BackupHistory(GetHistoryJson(command.Result)), null);
            }
        }
        catch (Exception e) when (ServerStore.ServerShutdown.IsCancellationRequested == false && RequestHandler.HttpContext.RequestAborted.IsCancellationRequested == false)
        {
            if (Logger.IsWarnEnabled)
                Logger.Warn($"Failed to get the backup history of '{DatabaseName}' from node '{nodeTag}'.", e);

            var error = token.IsCancellationRequested
                ? $"No answer within {ServerStore.Configuration.Cluster.OperationTimeout.AsTimeSpan.TotalSeconds} seconds"
                : e.AllInnerMessages();

            return (nodeTag, null, error);
        }
    }

    private static BlittableJsonReaderObject GetHistoryJson(BlittableJsonReaderObject response)
    {
        if (response == null || response.TryGet(nameof(BackupHistory), out BlittableJsonReaderObject history) == false || history == null)
            throw new InvalidOperationException($"The response has no '{nameof(BackupHistory)}'.");

        return history;
    }

    private string GetNodeOnlyPathAndQuery(BackupHistoryQuery query)
    {
        var pageSize = query.GroupBy == BackupHistoryGroupBy.None
            ? (int)Math.Min((long)query.Start + query.PageSize, int.MaxValue)
            : int.MaxValue;

        var request = RequestHandler.HttpContext.Request;
        var parameters = request.Query
            .Where(parameter => IsParameter(parameter.Key, RequestHandler.StartParameter) == false &&
                                IsParameter(parameter.Key, RequestHandler.PageSizeParameter) == false &&
                                IsParameter(parameter.Key, Constants.QueryString.NodeTag) == false)
            .SelectMany(parameter => parameter.Value.Select(value => KeyValuePair.Create(parameter.Key, value)))
            .Append(KeyValuePair.Create(RequestHandler.StartParameter, "0"))
            .Append(KeyValuePair.Create(RequestHandler.PageSizeParameter, pageSize.ToString()));

        return QueryHelpers.AddQueryString(request.Path, parameters);

        static bool IsParameter(string key, string name) => string.Equals(key, name, StringComparison.OrdinalIgnoreCase);
    }
}
