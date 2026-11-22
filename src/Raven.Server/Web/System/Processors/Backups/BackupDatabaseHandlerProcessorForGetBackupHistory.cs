using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.PeriodicBackup.BackupHistory;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.Backups;

internal sealed class BackupDatabaseHandlerProcessorForGetBackupHistory : AbstractBackupHistoryHandlerProcessor
{
    public BackupDatabaseHandlerProcessorForGetBackupHistory([NotNull] RequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool ShardNumberRequired => false;

    protected override async ValueTask WriteResponseAsync(TransactionOperationContext context, RawDatabaseRecord databaseRecord, int? shardNumber)
    {
        var includeIncrementals = RequestHandler.GetBoolValueQueryString("includeIncrementals", required: false) ?? true;
        var requestedTaskId = RequestHandler.GetLongQueryString("taskId", required: false);
        var fullBackupTicks = RequestHandler.GetLongQueryString("fullBackupTicks", required: false);

        if (fullBackupTicks.HasValue && requestedTaskId.HasValue == false)
            throw new BadRequestException("'taskId' must be specified when 'fullBackupTicks' is specified");

        var query = new BackupHistoryQuery
        {
            ShardNumber = shardNumber,
            TaskId = requestedTaskId,
            FullBackupTicks = fullBackupTicks,
            IncludeIncrementals = includeIncrementals,
            From = GetUtcDateTimeQueryString("from"),
            To = GetUtcDateTimeQueryString("to"),
            TaskName = RequestHandler.GetStringQueryString("taskName", required: false),
            Triggers = RequestHandler.GetEnumValuesQueryString<BackupTrigger>("trigger"),
            BackupTypes = RequestHandler.GetEnumValuesQueryString<BackupType>("backupType"),
            Start = RequestHandler.GetStart(),
            PageSize = RequestHandler.GetPageSize()
        };

        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(BackupHistory));
            writer.WriteObject(BackupHistoryStorage.GetBackupHistory(context, databaseRecord, query));
            writer.WriteEndObject();
        }
    }

    private DateTime? GetUtcDateTimeQueryString(string name)
    {
        var value = RequestHandler.GetDateTimeQueryString(name, required: false);
        return value?.Kind == DateTimeKind.Local ? value.Value.ToUniversalTime() : value;
    }
}
