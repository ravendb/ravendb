using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Alerts
{
    public class AlertsStorage
    {
        private readonly ServerStore _store;
        protected readonly Logger Logger;

        private StorageEnvironment _environment;

        private TransactionContextPool _contextPool;

        private readonly TableSchema _alertsSchema = new TableSchema();

        public AlertsStorage(string resourceName, ServerStore store)
        {
            _store = store;
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
                _alertsSchema.Create(tx, AlertsSchema.AlertsTree);

                tx.Commit();
            }

           
        }

        public unsafe void AddAlert(Alert alert)
        {

            //TODO: send notification

            if (Logger.IsInfoEnabled)
                Logger.Info($"Saving alert '{alert.Id}'.");

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                _store?.TrackChange(context, "AlertRaised", alert.Key);

                var table = tx.InnerTransaction.OpenTable(_alertsSchema, AlertsSchema.AlertsTree);

                var alertId = alert.Id;

                var alertAsJson = alert.ToJson();

                // if previous alert has dismissed until value pass this value to newly saved alert
                Slice slice;
                using (Slice.From(tx.InnerTransaction.Allocator, alertId, out slice))
                {
                    var existingTvr = table.ReadByKey(slice);
                    if (existingTvr != null)
                    {
                        var existingAlert = Read(context, existingTvr);

                        object dismissedUntilValue;
                        existingAlert.TryGetMember(nameof(alert.DismissedUntil), out dismissedUntilValue);
                        if (dismissedUntilValue != null)
                        {
                            var dismissedUntil = (LazyStringValue)dismissedUntilValue;
                            alertAsJson[nameof(alert.DismissedUntil)] = dismissedUntil;
                        }
                    }
                }
                

                using (var id = context.GetLazyString(alertId))
                using (var json = context.ReadObject(alertAsJson, "Alert", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var tvb = new TableValueBuilder
                    {
                        { id.Buffer, id.Size },
                        { json.BasePointer, json.Size }
                    };

                    table.Set(tvb);
                }

                tx.Commit();
            }
        }

        public void WriteAlerts(BlittableJsonTextWriter writer)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_alertsSchema, AlertsSchema.AlertsTree);

                writer.WriteStartArray();

                var first = true;

                foreach (var alertsTvr in table.SeekByPrimaryKey(Slices.BeforeAllKeys))
                {
                    if (first == false)
                        writer.WriteComma();

                    first = false;

                    var alert = Read(context, alertsTvr);
                    writer.WriteObject(alert);
                }

                writer.WriteEndArray();
            }
        }

        public void DeleteAlert(AlertType type, string key)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Deleteing alert '{type}'.");

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                _store?.TrackChange(context, "AlertDeleted", key);

                var table = tx.InnerTransaction.OpenTable(_alertsSchema, AlertsSchema.AlertsTree);

                var alertId = Alert.CreateId(type, key);

                Slice alertSlice;
                using (Slice.From(tx.InnerTransaction.Allocator, alertId, out alertSlice))
                {
                    table.DeleteByKey(alertSlice);
                }

                tx.Commit();
            }
        }

        private unsafe BlittableJsonReaderObject Read(JsonOperationContext context, TableValueReader reader)
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