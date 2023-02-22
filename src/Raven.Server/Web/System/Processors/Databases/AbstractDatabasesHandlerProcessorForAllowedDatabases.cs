using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.Databases;

internal abstract class AbstractDatabasesHandlerProcessorForAllowedDatabases<TResult> : AbstractServerHandlerProxyReadProcessor<TResult>
{
    protected AbstractDatabasesHandlerProcessorForAllowedDatabases([NotNull] RequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected string GetName() => RequestHandler.GetStringQueryString("name", required: false);

    protected int GetStart() => RequestHandler.GetStart();

    protected int GetPageSize() => RequestHandler.GetPageSize();

    protected override Task HandleRemoteNodeAsync(ProxyCommand<TResult> command, JsonOperationContext context, OperationCancelToken token)
    {
        return RequestHandler.ServerStore.ClusterRequestExecutor.ExecuteAsync(command, context, token: token.Token);
    }

    protected static BackupInfo GetBackupInfo(string databaseName, RawDatabaseRecord record, DocumentDatabase database, ServerStore serverStore, TransactionOperationContext context)
    {
        if (database == null)
        {
            var periodicBackups = new List<PeriodicBackup>();

            foreach (var periodicBackupConfiguration in record.PeriodicBackupsConfiguration)
            {
                periodicBackups.Add(new PeriodicBackup
                {
                    Configuration = periodicBackupConfiguration,
                    BackupStatus = BackupUtils.GetBackupStatusFromCluster(serverStore, context, databaseName, periodicBackupConfiguration.TaskId)
                });
            }

            return BackupUtils.GetBackupInfo(new BackupUtils.BackupInfoParameters
            {
                ServerStore = serverStore,
                PeriodicBackups = periodicBackups,
                DatabaseName = databaseName,
                Context = context
            });
        }

        return database.PeriodicBackupRunner.GetBackupInfo(context);
    }

    protected static TimeSpan? GetUpTime(DocumentDatabase database)
    {
        if (database == null)
            return null;

        return SystemTime.UtcNow - database.StartTime;
    }

    protected static IndexRunningStatus? GetIndexingStatus(RawDatabaseRecord record, DocumentDatabase database)
    {
        var indexingStatus = database?.IndexStore?.Status;
        if (indexingStatus == null)
        {
            // Looking for disabled indexing flag inside the database settings for offline database status
            if (record.Settings.TryGetValue(RavenConfiguration.GetKey(x => x.Indexing.Disabled), out var val) &&
                bool.TryParse(val, out var indexingDisabled) && indexingDisabled)
                indexingStatus = IndexRunningStatus.Disabled;
        }

        return indexingStatus;
    }

    protected static StudioConfiguration.StudioEnvironment GetStudioEnvironment(RawDatabaseRecord record)
    {
        if (record.StudioConfiguration == null || record.StudioConfiguration.Disabled)
            return StudioConfiguration.StudioEnvironment.None;

        return record.StudioConfiguration.Environment;
    }

    protected async IAsyncEnumerable<RawDatabaseRecord> GetAllowedDatabaseRecordsAsync(string name, TransactionOperationContext context, int start, int pageSize)
    {
        IEnumerable<RawDatabaseRecord> items;
        if (name != null)
        {
            var databaseRecord = ServerStore.Cluster.ReadRawDatabaseRecord(context, name, out long _);
            if (databaseRecord == null)
                yield break;

            items = new[] { databaseRecord };
        }
        else
        {
            items = ServerStore.Cluster.GetAllRawDatabases(context, start, pageSize);
        }

        var allowedDbs = await RequestHandler.GetAllowedDbsAsync(null, requireAdmin: false, requireWrite: false);

        if (allowedDbs.HasAccess == false)
            yield break;

        if (allowedDbs.AuthorizedDatabases != null)
        {
            items = items.Where(item => allowedDbs.AuthorizedDatabases.ContainsKey(item.DatabaseName));
        }

        foreach (var item in items)
            yield return item;
    }
}
