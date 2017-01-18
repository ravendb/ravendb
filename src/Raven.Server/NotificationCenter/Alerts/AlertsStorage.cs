using System;
using System.Collections.Generic;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.NotificationCenter.Alerts
{
    public class AlertsStorage
    {
        protected readonly Logger Logger;

        private StorageEnvironment _environment;

        private TransactionContextPool _contextPool;

        private readonly TableSchema _alertsSchema = new TableSchema();

        public AlertsStorage(string resourceName)
        {
            Logger = LoggingSource.Instance.GetLogger<AlertsStorage>(resourceName);

            _alertsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
            });
        }

        public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
        {
            _environment = environment;
            _contextPool = contextPool;

            TransactionOperationContext context;
            using (contextPool.AllocateOperationContext(out context))
            using (var tx = _environment.WriteTransaction(context.PersistentContext))
            {
                _alertsSchema.Create(tx, AlertsSchema.AlertsTree, 16);

                tx.Commit();
            }
        }

        public void AddAlert(IAlert alert)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Saving alert '{alert.Id}'.");

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                AddAlert(alert, context, tx);

                tx.Commit();
            }
        }

        public unsafe void AddAlert(IAlert alert, TransactionOperationContext context, RavenTransaction tx)
        {
            var table = tx.InnerTransaction.OpenTable(_alertsSchema, AlertsSchema.AlertsTree);

            var alertDjv = alert.ToJson();

            // if previous alert has dismissed until value pass this value to newly saved alert
            Slice slice;
            using (Slice.From(tx.InnerTransaction.Allocator, alert.Id, out slice))
            {
                TableValueReader existingTvr;
                if (table.ReadByKey(slice, out existingTvr))
                {
                    using (var existingAlert = Read(context, ref existingTvr))
                    {
                        object dismissedUntilValue;
                        existingAlert.TryGetMember(nameof(alert.DismissedUntil), out dismissedUntilValue);
                        if (dismissedUntilValue != null)
                        {
                            var dismissedUntil = (LazyStringValue) dismissedUntilValue;
                            alertDjv[nameof(alert.DismissedUntil)] = dismissedUntil;
                        }
                    }
                }
            }

            var lazyStringId = context.GetDiscardableLazyString(alert.Id);

            using (var json = context.ReadObject(alertDjv, "alert", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
            {
                var tvb = new TableValueBuilder
                {
                    {lazyStringId.Buffer, lazyStringId.Size},
                    {json.BasePointer, json.Size}
                };

                table.Set(tvb);
            }
        }

        public IDisposable ReadAlerts(out IEnumerable<BlittableJsonReaderObject> alerts)
        {
            using (var scope = new DisposeableScope())
            {
                TransactionOperationContext context;

                scope.EnsureDispose(_contextPool.AllocateOperationContext(out context));
                scope.EnsureDispose(context.OpenReadTransaction());

                alerts = ReadAlertsInternal(context);

                return scope.Delay();
            }
        }

        private IEnumerable<BlittableJsonReaderObject> ReadAlertsInternal(TransactionOperationContext context)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(_alertsSchema, AlertsSchema.AlertsTree);

            foreach (var alertsTvr in table.SeekByPrimaryKey(Slices.BeforeAllKeys))
            {
                yield return Read(context, alertsTvr);
            }
        }

        public void DeleteAlert(DatabaseAlertType type, string key)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Deleting alert '{type}'.");

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_alertsSchema, AlertsSchema.AlertsTree);

                var alertId = AlertUtil.CreateId(type, key); // TODO arek

                Slice alertSlice;
                using (Slice.From(tx.InnerTransaction.Allocator, alertId, out alertSlice))
                {
                    table.DeleteByKey(alertSlice);
                }

                tx.Commit();
            }
        }

        public long GetAlertCount()
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_alertsSchema, AlertsSchema.AlertsTree);
                return table.NumberOfEntries;
            }
        }

        private unsafe BlittableJsonReaderObject Read(JsonOperationContext context, ref TableValueReader reader)
        {
            int size;
            var ptr = reader.Read(AlertsSchema.AlertsTable.JsonIndex, out size);
            return new BlittableJsonReaderObject(ptr, size, context);
        }


        public static class AlertsSchema
        {
            public const string AlertsTree = "Alerts";

            public static class AlertsTable
            {
#pragma warning disable 169
                public const int IdIndex = 0;
                public const int JsonIndex = 1;
#pragma warning restore 169
            }
        }
    }
}