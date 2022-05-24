using System;
using System.Collections.Generic;
using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseOldestBackup : ScalarObjectBase<TimeTicks>
    {
        private readonly ServerStore _serverStore;
        
        public DatabaseOldestBackup(ServerStore serverStore)
            : base(SnmpOids.Databases.General.TimeSinceOldestBackup)
        {
            _serverStore = serverStore;
        }

        protected override TimeTicks GetData()
        {
            var timeOfOldestBackup = GetTimeSinceOldestBackup(_serverStore);
            return SnmpValuesHelper.TimeSpanToTimeTicks(timeOfOldestBackup);
        }

        private static TimeSpan GetTimeSinceOldestBackup(ServerStore serverStore)
        {
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var databaseNames = serverStore.Cluster.GetDatabaseNames(context);
                DateTime? LastBackup(string databaseName) => GetLastBackup(context, serverStore, databaseName);
                return GetTimeSinceOldestBackupInternal(databaseNames, LastBackup);
            }
        }

        internal static TimeSpan GetTimeSinceOldestBackupInternal(
            List<string> databaseNames, 
            Func<string, DateTime?> getLastBackup, 
            SystemTime systemTime = null)
        {
            var now = systemTime?.GetUtcNow() ?? SystemTime.UtcNow;
            DateTime oldestBackup = now;

            foreach (var databaseName in databaseNames)
            {
                var lastBackup = getLastBackup(databaseName);
                if (lastBackup == null)
                    continue;

                if (lastBackup < oldestBackup)
                    oldestBackup = lastBackup.Value;
            }
            
            if (now <= oldestBackup)
                return TimeSpan.Zero;

            return now - oldestBackup;
        }

        private static DateTime? GetLastBackup(TransactionOperationContext context, ServerStore serverStore, string databaseName)
        {
            using (var databaseRecord = serverStore.Cluster.ReadRawDatabaseRecord(context, databaseName, out _))
            {
                if (databaseRecord == null)
                    return null; // should not happen

                if (databaseRecord.IsDisabled)
                    return null; // we do not monitor disabled databases (RavenDB-15335)

                var periodicBackupTaskIds = databaseRecord.PeriodicBackupsTaskIds;
                if (periodicBackupTaskIds == null || periodicBackupTaskIds.Count == 0)
                    return null; // no backup

                var lastBackup = DateTime.MinValue;

                foreach (var periodicBackupTaskId in periodicBackupTaskIds)
                {
                    var status = BackupUtils.GetBackupStatusFromCluster(serverStore, context, databaseName, periodicBackupTaskId);
                    if (status == null)
                        continue; // we have a backup task but no backup was ever done

                    var currentLatestBackup = LastBackupDate(status.LastFullBackup, status.LastIncrementalBackup);
                    if (currentLatestBackup > lastBackup)
                        lastBackup = currentLatestBackup;
                }

                return lastBackup;

                static DateTime LastBackupDate(DateTime? fullBackup, DateTime? incrementalBackup)
                {
                    if (fullBackup == null)
                        return DateTime.MinValue; // we never did a full backup

                    if (incrementalBackup == null)
                        return fullBackup.Value; // no incremental backup

                    return incrementalBackup > fullBackup ? incrementalBackup.Value : fullBackup.Value;
                }
            }
        }
    }
}
