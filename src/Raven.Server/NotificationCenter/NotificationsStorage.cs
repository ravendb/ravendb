using System;
using System.Collections.Generic;
using Raven.Client.Util;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.NotificationCenter
{
    public unsafe class NotificationsStorage
    {
        private static readonly Slice ByCreatedAt;

        private static readonly Slice ByPostponedUntil;

        protected readonly Logger Logger;

        private StorageEnvironment _environment;

        private TransactionContextPool _contextPool;

        internal readonly TableSchema _actionsSchema = new TableSchema();

        static NotificationsStorage()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "ByCreatedAt", ByteStringType.Immutable, out ByCreatedAt);
                Slice.From(ctx, "ByPostponedUntil", ByteStringType.Immutable, out ByPostponedUntil);
            }
        }

        public NotificationsStorage(string resourceName)
        {
            Logger = LoggingSource.Instance.GetLogger<NotificationsStorage>(resourceName);

            _actionsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = NotificationsSchema.NotificationsTable.IdIndex,
                Count = 1
            });

            _actionsSchema.DefineIndex(new TableSchema.SchemaIndexDef // might be the same ticks, so duplicates are allowed - cannot use fixed size index
            {
                StartIndex = NotificationsSchema.NotificationsTable.CreatedAtIndex,
                Name = ByCreatedAt
            });

            _actionsSchema.DefineIndex(new TableSchema.SchemaIndexDef // might be the same ticks, so duplicates are allowed - cannot use fixed size index
            {
                StartIndex = NotificationsSchema.NotificationsTable.PostponedUntilIndex,
                Name = ByPostponedUntil
            });
        }

        public void Initialize(StorageEnvironment environment, TransactionContextPool contextPool)
        {
            _environment = environment;
            _contextPool = contextPool;

            using (contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = _environment.WriteTransaction(context.PersistentContext))
            {
                _actionsSchema.Create(tx, NotificationsSchema.NotificationsTree, 16);

                tx.Commit();
            }

            Cleanup();
        }

        public bool Store(Notification notification, DateTime? postponeUntil = null, bool updateExisting = true)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                using (var tx = context.OpenReadTransaction())
                {
                    // if previous notification had postponed until value pass this value to newly saved notification
                    var existing = Get(notification.Id, context, tx);

                    if (existing != null && updateExisting == false)
                        return false;

                    if (postponeUntil == null)
                    {
                        if (existing?.PostponedUntil == DateTime.MaxValue) // postponed until forever
                            return false;

                        if (existing?.PostponedUntil != null && existing.PostponedUntil.Value > SystemTime.UtcNow)
                            postponeUntil = existing.PostponedUntil;
                    }
                }

                if (Logger.IsInfoEnabled)
                    Logger.Info($"Saving notification '{notification.Id}'.");

                using (var json = context.ReadObject(notification.ToJson(), "notification", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                using (var tx = context.OpenWriteTransaction())
                {
                    Store(context.GetLazyString(notification.Id), notification.CreatedAt, postponeUntil, json, tx);
                    tx.Commit();
                }
            }

            return true;
        }

        private readonly long _postponeDateNotSpecified = Bits.SwapBytes(long.MaxValue);

        private void Store(LazyStringValue id, DateTime createdAt, DateTime? postponedUntil, BlittableJsonReaderObject action, RavenTransaction tx)
        {
            var table = tx.InnerTransaction.OpenTable(_actionsSchema, NotificationsSchema.NotificationsTree);

            var createdAtTicks = Bits.SwapBytes(createdAt.Ticks);

            var postponedUntilTicks = postponedUntil != null
                ? Bits.SwapBytes(postponedUntil.Value.Ticks)
                : _postponeDateNotSpecified;

            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(id.Buffer, id.Size);
                tvb.Add((byte*)&createdAtTicks, sizeof(long));
                tvb.Add((byte*)&postponedUntilTicks, sizeof(long));
                tvb.Add(action.BasePointer, action.Size);

                table.Set(tvb);
            }
        }

        public IDisposable ReadActionsOrderedByCreationDate(out IEnumerable<NotificationTableValue> actions)
        {
            using (var scope = new DisposableScope())
            {
                scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
                scope.EnsureDispose(context.OpenReadTransaction());

                actions = ReadActionsByCreatedAtIndex(context);

                return scope.Delay();
            }
        }

        public IDisposable Read(string id, out NotificationTableValue value)
        {
            using (var scope = new DisposableScope())
            {
                RavenTransaction tx;

                scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
                scope.EnsureDispose(tx = context.OpenReadTransaction());

                value = Get(id, context, tx);

                return scope.Delay();
            }
        }

        private IEnumerable<NotificationTableValue> ReadActionsByCreatedAtIndex(TransactionOperationContext context)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(_actionsSchema, NotificationsSchema.NotificationsTree);

            foreach (var tvr in table.SeekForwardFrom(_actionsSchema.Indexes[ByCreatedAt], Slices.BeforeAllKeys, 0))
            {
                yield return Read(context, ref tvr.Result.Reader);
            }
        }

        public IDisposable ReadPostponedActions(out IEnumerable<NotificationTableValue> actions, DateTime cutoff)
        {
            using (var scope = new DisposableScope())
            {

                scope.EnsureDispose(_contextPool.AllocateOperationContext(out TransactionOperationContext context));
                scope.EnsureDispose(context.OpenReadTransaction());

                actions = ReadPostponedActionsByPostponedUntilIndex(context, cutoff);

                return scope.Delay();
            }
        }

        private IEnumerable<NotificationTableValue> ReadPostponedActionsByPostponedUntilIndex(TransactionOperationContext context, DateTime cutoff)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(_actionsSchema, NotificationsSchema.NotificationsTree);

            foreach (var tvr in table.SeekForwardFrom(_actionsSchema.Indexes[ByPostponedUntil], Slices.BeforeAllKeys, 0))
            {
                var action = Read(context, ref tvr.Result.Reader);

                if (action.PostponedUntil == null)
                    continue;

                if (action.PostponedUntil > cutoff)
                    break;

                if (action.PostponedUntil == DateTime.MaxValue)
                    break;

                yield return action;
            }
        }

        private NotificationTableValue Get(string id, JsonOperationContext context, RavenTransaction tx)
        {
            var table = tx.InnerTransaction.OpenTable(_actionsSchema, NotificationsSchema.NotificationsTree);

            using (Slice.From(tx.InnerTransaction.Allocator, id, out Slice slice))
            {
                if (table.ReadByKey(slice, out TableValueReader tvr) == false)
                    return null;

                return Read(context, ref tvr);
            }
        }

        public bool Delete(string id, RavenTransaction existingTransaction = null)
        {
            bool deleteResult;

            if (existingTransaction != null)
            {
                deleteResult = DeleteFromTable(existingTransaction);
            }
            else
            {
                using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    deleteResult = DeleteFromTable(tx);
                    tx.Commit();
                }
            }

            if (deleteResult && Logger.IsInfoEnabled)
                Logger.Info($"Deleted notification '{id}'.");

            return deleteResult;

            bool DeleteFromTable(RavenTransaction tx)
            {
                var table = tx.InnerTransaction.OpenTable(_actionsSchema, NotificationsSchema.NotificationsTree);

                using (Slice.From(tx.InnerTransaction.Allocator, id, out Slice alertSlice))
                {
                    return table.DeleteByKey(alertSlice);
                }
            }
        }

        public long GetAlertCount()
        {
            return GetNotificationCount(nameof(NotificationType.AlertRaised));
        }

        public long GetPerformanceHintCount()
        {
            return GetNotificationCount(nameof(NotificationType.PerformanceHint));
        }

        private long GetNotificationCount(string notificationType)
        {
            var count = 0;

            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var action in ReadActionsByCreatedAtIndex(context))
                {
                    if (action.Json.TryGetMember(nameof(Notification.Type), out object type) == false)
                        ThrowCouldNotFindNotificationType(action);

                    var typeLsv = (LazyStringValue)type;

                    if (typeLsv.CompareTo(notificationType) == 0)
                        count++;
                }
            }

            return count;
        }

        private NotificationTableValue Read(JsonOperationContext context, ref TableValueReader reader)
        {

            var createdAt = new DateTime(Bits.SwapBytes(*(long*)reader.Read(NotificationsSchema.NotificationsTable.CreatedAtIndex, out int size)));

            var postponeUntilTicks = *(long*)reader.Read(NotificationsSchema.NotificationsTable.PostponedUntilIndex, out size);

            DateTime? postponedUntil = null;
            if (postponeUntilTicks != _postponeDateNotSpecified)
                postponedUntil = new DateTime(Bits.SwapBytes(postponeUntilTicks));

            var jsonPtr = reader.Read(NotificationsSchema.NotificationsTable.JsonIndex, out size);

            return new NotificationTableValue
            {
                CreatedAt = createdAt,
                PostponedUntil = postponedUntil,
                Json = new BlittableJsonReaderObject(jsonPtr, size, context)
            };
        }

        public string GetDatabaseFor(string id)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                var item = Get(id, context, tx);
                if (item == null)
                    return null;
                item.Json.TryGet("Database", out string db);
                return db;
            }
        }

        public void ChangePostponeDate(string id, DateTime? postponeUntil)
        {
            using (_contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var item = Get(id, context, tx);

                if (item == null)
                    return;

                var itemCopy = context.GetMemory(item.Json.Size);

                Memory.Copy(itemCopy.Address, item.Json.BasePointer, item.Json.Size);

                Store(context.GetLazyString(id), item.CreatedAt, postponeUntil,
                    //we create a copy because we can't update directly from mutated memory
                    new BlittableJsonReaderObject(itemCopy.Address, item.Json.Size, context)
                    , tx);

                tx.Commit();
            }
        }

        private void Cleanup()
        {
            RemoveNewVersionAvailableAlertIfNecessary();
        }

        private void RemoveNewVersionAvailableAlertIfNecessary()
        {
            var buildNumber = ServerVersion.Build;

            var id = AlertRaised.GetKey(AlertType.Server_NewVersionAvailable, null);
            using (Read(id, out var ntv))
            {
                if (ntv == null)
                    return;

                var delete = true;

                if (buildNumber != ServerVersion.DevBuildNumber)
                {
                    if (ntv.Json.TryGetMember(nameof(AlertRaised.Details), out var o)
                        && o is BlittableJsonReaderObject detailsJson)
                    {
                        if (detailsJson.TryGetMember(nameof(NewVersionAvailableDetails.VersionInfo), out o)
                            && o is BlittableJsonReaderObject newVersionDetailsJson)
                        {
                            var value = JsonDeserializationServer.LatestVersionCheckVersionInfo(newVersionDetailsJson);
                            delete = value.BuildNumber <= buildNumber;
                        }
                    }
                }

                if (delete)
                    Delete(id);
            }
        }

        private static void ThrowCouldNotFindNotificationType(NotificationTableValue action)
        {
            string notificationJson;

            try
            {
                notificationJson = action.Json.ToString();
            }
            catch (Exception e)
            {
                notificationJson = $"invalid json - {e.Message}";
            }

            throw new InvalidOperationException(
                $"Could not find notification type. Notification: {notificationJson}, created at: {action.CreatedAt}, postponed until: {action.PostponedUntil}");
        }

        public static class NotificationsSchema
        {
            public const string NotificationsTree = "Notifications";

            public static class NotificationsTable
            {
#pragma warning disable 169
                public const int IdIndex = 0;
                public const int CreatedAtIndex = 1;
                public const int PostponedUntilIndex = 2;
                public const int JsonIndex = 3;
#pragma warning restore 169
            }
        }
    }
}
