using System;
using System.Collections.Generic;
using Raven.Abstractions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;
using Action = Raven.Server.NotificationCenter.Actions.Action;

namespace Raven.Server.NotificationCenter
{
    public unsafe class ActionsStorage
    {
        protected readonly Logger Logger;

        private StorageEnvironment _environment;

        private TransactionContextPool _contextPool;

        private readonly TableSchema _actionsSchema = new TableSchema();

        public ActionsStorage(string resourceName)
        {
            Logger = LoggingSource.Instance.GetLogger<ActionsStorage>(resourceName);

            _actionsSchema.DefineKey(new TableSchema.SchemaIndexDef
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
                _actionsSchema.Create(tx, ActionsSchema.ActionsTree, 16);

                tx.Commit();
            }
        }

        public void Store(Action action)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Saving action '{action.Id}'.");

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                // if previous action has dismissed until value pass this value to newly saved action
                var existing = Get(action.Id, context, tx);

                if (existing != null)
                {
                    DateTime dismissedUntil;
                    if (TryGetDismissedUntilDate(existing, out dismissedUntil))
                    {
                        if (action.DismissedUntil == null && dismissedUntil > SystemTime.UtcNow)
                            action.DismissedUntil = dismissedUntil;
                    }
                }

                using (var json = context.ReadObject(action.ToJson(), "action", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    Store(context.GetDiscardableLazyString(action.Id), json, tx);
                }

                tx.Commit();
            }
        }

        private void Store(LazyStringValue id, BlittableJsonReaderObject action, RavenTransaction tx)
        {
            var table = tx.InnerTransaction.OpenTable(_actionsSchema, ActionsSchema.ActionsTree);

            var tvb = new TableValueBuilder
                {
                    {id.Buffer, id.Size},
                    {action.BasePointer, action.Size}
                };

            table.Set(tvb);
        }

        private bool TryGetDismissedUntilDate(BlittableJsonReaderObject action, out DateTime dismissedUntil)
        {
            object dismissedUntilString;
            if (action.TryGetMember(nameof(Action.DismissedUntil), out dismissedUntilString) == false || dismissedUntilString == null)
            {
                dismissedUntil = default(DateTime);
                return false;
            }

            var lazyStrinDate = (LazyStringValue) dismissedUntilString;

            DateTimeOffset _;
            var parsedType = LazyStringParser.TryParseDateTime(lazyStrinDate.Buffer, lazyStrinDate.Size, out dismissedUntil, out _);

            switch (parsedType)
            {
                case LazyStringParser.Result.DateTime:
                    return true;
                case LazyStringParser.Result.DateTimeOffset:
                    throw new NotSupportedException($"Got {nameof(DateTimeOffset)} while only {nameof(DateTime)} is supported");
                default:
                    throw new NotSupportedException($"Unknown type: {parsedType}");
            }
        }

        public IDisposable ReadActions(out IEnumerable<BlittableJsonReaderObject> actions)
        {
            using (var scope = new DisposeableScope())
            {
                TransactionOperationContext context;

                scope.EnsureDispose(_contextPool.AllocateOperationContext(out context));
                scope.EnsureDispose(context.OpenReadTransaction());

                actions = ReadActionsInternal(context);

                return scope.Delay();
            }
        }

        private IEnumerable<BlittableJsonReaderObject> ReadActionsInternal(TransactionOperationContext context)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(_actionsSchema, ActionsSchema.ActionsTree);

            foreach (var alertsTvr in table.SeekByPrimaryKey(Slices.BeforeAllKeys))
            {
                yield return Read(context, alertsTvr);
            }
        }

        private BlittableJsonReaderObject Get(string id, TransactionOperationContext context, RavenTransaction tx)
        {
            var table = tx.InnerTransaction.OpenTable(_actionsSchema, ActionsSchema.ActionsTree);

            Slice slice;
            using (Slice.From(tx.InnerTransaction.Allocator, id, out slice))
            {
                var read = table.ReadByKey(slice);

                if (read == null)
                    return null;

                return Read(context, read);
            }
        }

        public bool Delete(string id)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Deleting action '{id}'.");

            bool deleteResult;

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_actionsSchema, ActionsSchema.ActionsTree);

                Slice alertSlice;
                using (Slice.From(tx.InnerTransaction.Allocator, id, out alertSlice))
                {
                    deleteResult = table.DeleteByKey(alertSlice);
                }

                tx.Commit();
            }

            return deleteResult;
        }

        public long GetAlertCount() // TODO: only alerts
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenReadTransaction())
            {
                var table = tx.InnerTransaction.OpenTable(_actionsSchema, ActionsSchema.ActionsTree);
                return table.NumberOfEntries;
            }
        }

        private BlittableJsonReaderObject Read(JsonOperationContext context, TableValueReader reader)
        {
            int size;
            var ptr = reader.Read(ActionsSchema.ActionsTable.JsonIndex, out size);
            return new BlittableJsonReaderObject(ptr, size, context);
        }

        public void ChangeDismissUntilDate(string id, DateTime dismissUntil)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var item = Get(id, context, tx);

                if (item == null)
                    return;

                item.Modifications = new DynamicJsonValue
                {
                    [nameof(Action.DismissedUntil)] = dismissUntil
                };

                var updated = context.ReadObject(item, "action", BlittableJsonDocumentBuilder.UsageMode.ToDisk);

                Store(context.GetDiscardableLazyString(id), updated, tx);

                tx.Commit();
            }
        }

        public static class ActionsSchema
        {
            public const string ActionsTree = "Actions";

            public static class ActionsTable
            {
#pragma warning disable 169
                public const int IdIndex = 0;
                public const int JsonIndex = 1;
#pragma warning restore 169
            }
        }
    }
}