using System;
using System.Collections.Generic;
using Raven.Abstractions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
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
        private static readonly Slice ByCreatedAt;

        protected readonly Logger Logger;

        private StorageEnvironment _environment;

        private TransactionContextPool _contextPool;

        private readonly TableSchema _actionsSchema = new TableSchema();

        static ActionsStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "ByCreatedAt", ByteStringType.Immutable, out ByCreatedAt);
        }

        public ActionsStorage(string resourceName)
        {
            Logger = LoggingSource.Instance.GetLogger<ActionsStorage>(resourceName);

            _actionsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1
            });

            _actionsSchema.DefineIndex(new TableSchema.SchemaIndexDef // might be the same ticks, so duplicates are allowed - cannot use fixed size index
            {
                StartIndex = 1,
                Name = ByCreatedAt
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
                // if previous action had postponed until value pass this value to newly saved action
                var existing = Get(action.Id, context, tx);

                if (existing != null)
                {
                    DateTime postponedUntil;
                    if (TryReadDate(existing, nameof(Action.PostponedUntil), out postponedUntil))
                    {
                        if (action.PostponedUntil == null && postponedUntil > SystemTime.UtcNow)
                            action.PostponedUntil = postponedUntil;
                    }
                }

                using (var json = context.ReadObject(action.ToJson(), "action", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    Store(context.GetDiscardableLazyString(action.Id), action.CreatedAt, json, tx);
                }

                tx.Commit();
            }
        }

        private void Store(LazyStringValue id, DateTime createdAt, BlittableJsonReaderObject action, RavenTransaction tx)
        {
            var table = tx.InnerTransaction.OpenTable(_actionsSchema, ActionsSchema.ActionsTree);

            var createdAtTicks = Bits.SwapBytes(createdAt.Ticks);

            var tvb = new TableValueBuilder
                {
                    {id.Buffer, id.Size},
                    {(byte*)&createdAtTicks, sizeof(long)},
                    {action.BasePointer, action.Size}
                };

            table.Set(tvb);
        }

        internal static bool TryReadDate(BlittableJsonReaderObject action, string fieldName, out DateTime date)
        {
            object dateString;
            if (action.TryGetMember(fieldName, out dateString) == false || dateString == null)
            {
                date = default(DateTime);
                return false;
            }

            var lazyStringDate = (LazyStringValue) dateString;

            DateTimeOffset _;
            var parsedType = LazyStringParser.TryParseDateTime(lazyStringDate.Buffer, lazyStringDate.Size, out date, out _);

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

        public IDisposable ReadActionsOrderedByCreationDate(out IEnumerable<BlittableJsonReaderObject> actions)
        {
            using (var scope = new DisposeableScope())
            {
                TransactionOperationContext context;

                scope.EnsureDispose(_contextPool.AllocateOperationContext(out context));
                scope.EnsureDispose(context.OpenReadTransaction());

                actions = ReadActionsByCreatedAtIndex(context);

                return scope.Delay();
            }
        }

        private IEnumerable<BlittableJsonReaderObject> ReadActionsByCreatedAtIndex(TransactionOperationContext context)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(_actionsSchema, ActionsSchema.ActionsTree);
            
            foreach (var it in table.SeekForwardFrom(_actionsSchema.Indexes[ByCreatedAt], Slices.BeforeAllKeys))
            {
                foreach (var tvr in it.Results)
                {
                    yield return Read(context, tvr);
                }
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

        public void ChangePostponeDate(string id, DateTime postponeUntil)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var item = Get(id, context, tx);

                if (item == null)
                    return;

                DateTime createdAt;
                if (TryReadDate(item, nameof(Action.CreatedAt), out createdAt) == false)
                    throw new InvalidOperationException($"Stored action does not have created at date. Action: {item}");

                item.Modifications = new DynamicJsonValue
                {
                    [nameof(Action.PostponedUntil)] = postponeUntil
                };

                var updated = context.ReadObject(item, "action", BlittableJsonDocumentBuilder.UsageMode.ToDisk);
                
                Store(context.GetDiscardableLazyString(id), createdAt, updated, tx);

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
                public const int CreatedAtIndex = 1;
                public const int JsonIndex = 2;
#pragma warning restore 169
            }
        }
    }
}