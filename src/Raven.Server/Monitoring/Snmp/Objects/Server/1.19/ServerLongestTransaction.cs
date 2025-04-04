using System;
using System.Collections.Generic;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.ServerWide;
using Voron;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerLongestTransaction : ScalarObjectBase<TimeTicks>
    {
        private readonly ServerStore _store;

        public ServerLongestTransaction(ServerStore store)
            : base(SnmpOids.Server.ServerLongestTransaction)
        {
            _store = store;
        }

        protected override TimeTicks GetData()
        {
            var now = SystemTime.UtcNow;
            TryGetOldestTransaction(_store._env, out var oldestTransaction);

            foreach (var storageEnvironment in GetAllStorageEnvironmentsForLoadedDatabases())
            {
                if (TryGetOldestTransaction(storageEnvironment, out var databaseOldestTransaction) == false)
                    continue;

                if (oldestTransaction == DateTime.MinValue)
                {
                    oldestTransaction = databaseOldestTransaction;
                    continue;
                }

                if (databaseOldestTransaction < oldestTransaction)
                    oldestTransaction = databaseOldestTransaction;
            }

            if (oldestTransaction == DateTime.MinValue)
                return SnmpValuesHelper.TimeTicksZero;

            var transactionAge = now - oldestTransaction;
            return SnmpValuesHelper.TimeSpanToTimeTicks(transactionAge.TotalMilliseconds > 0 ? transactionAge : TimeSpan.Zero);
        }

        private IEnumerable<StorageEnvironment> GetAllStorageEnvironmentsForLoadedDatabases()
        {
            foreach (var kvp in _store.DatabasesLandlord.DatabasesCache)
            {
                var databaseTask = kvp.Value;

                if (databaseTask == null || databaseTask.IsCompletedSuccessfully == false)
                    continue;

                var database = databaseTask.Result;

                foreach (var storageEnvironmentWithType in database.GetAllStoragesEnvironment())
                    yield return storageEnvironmentWithType.Environment;
            }
        }

        private static bool TryGetOldestTransaction(StorageEnvironment env, out DateTime oldestTransaction)
        {
            oldestTransaction = DateTime.MinValue;

            try
            {
                var allTransactionsInstances = env.ActiveTransactions.AllTransactionsInstances;
                if (allTransactionsInstances.Count == 0)
                    return false;

                oldestTransaction = allTransactionsInstances.Min(x => x.TxStartTime);
                return true;
            }
            catch
            {
                return false;
            }
        }
    }
}
