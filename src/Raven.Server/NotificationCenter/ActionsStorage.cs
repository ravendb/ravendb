using System;
using System.Collections.Generic;
using Raven.Abstractions;
using Raven.Server.NotificationCenter.Actions;
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

        private static readonly Slice ByPostponedUntil;

        protected readonly Logger Logger;

        private StorageEnvironment _environment;

        private TransactionContextPool _contextPool;

        private readonly TableSchema _actionsSchema = new TableSchema();

        static ActionsStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "ByCreatedAt", ByteStringType.Immutable, out ByCreatedAt);
            Slice.From(StorageEnvironment.LabelsContext, "ByPostponedUntil", ByteStringType.Immutable, out ByPostponedUntil);
        }

        public ActionsStorage(string resourceName)
        {
            Logger = LoggingSource.Instance.GetLogger<ActionsStorage>(resourceName);

            _actionsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = ActionsSchema.ActionsTable.IdIndex,
                Count = 1
            });

            _actionsSchema.DefineIndex(new TableSchema.SchemaIndexDef // might be the same ticks, so duplicates are allowed - cannot use fixed size index
            {
                StartIndex = ActionsSchema.ActionsTable.CreatedAtIndex,
                Name = ByCreatedAt
            });

            _actionsSchema.DefineIndex(new TableSchema.SchemaIndexDef // might be the same ticks, so duplicates are allowed - cannot use fixed size index
            {
                StartIndex = ActionsSchema.ActionsTable.PostponedUntilIndex,
                Name = ByPostponedUntil
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

                DateTime? postponeUntil = null;

                if (existing?.PostponedUntil != null && existing.PostponedUntil.Value > SystemTime.UtcNow)
                    postponeUntil= existing.PostponedUntil;

                using (var json = context.ReadObject(action.ToJson(), "action", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    Store(context.GetDiscardableLazyString(action.Id), action.CreatedAt, postponeUntil, json, tx);
                }

                tx.Commit();
            }
        }

        private readonly long _postponeDateNotSpecified = Bits.SwapBytes(long.MaxValue);

        private void Store(LazyStringValue id, DateTime createdAt, DateTime? postponedUntil, BlittableJsonReaderObject action, RavenTransaction tx)
        {
            var table = tx.InnerTransaction.OpenTable(_actionsSchema, ActionsSchema.ActionsTree);

            var createdAtTicks = Bits.SwapBytes(createdAt.Ticks);

            var postponedUntilTicks = postponedUntil != null
                ? Bits.SwapBytes(postponedUntil.Value.Ticks)
                : _postponeDateNotSpecified;

            var tvb = new TableValueBuilder
                {
                    {id.Buffer, id.Size},
                    {(byte*)&createdAtTicks, sizeof(long)},
                    {(byte*)&postponedUntilTicks, sizeof(long)},
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

        public IDisposable ReadActionsOrderedByCreationDate(out IEnumerable<ActionTableValue> actions)
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

        public IDisposable Read(string id, out ActionTableValue value)
        {
            using (var scope = new DisposeableScope())
            {
                TransactionOperationContext context;
                RavenTransaction tx;

                scope.EnsureDispose(_contextPool.AllocateOperationContext(out context));
                scope.EnsureDispose(tx = context.OpenReadTransaction());

                value = Get(id, context, tx);

                return scope.Delay();
            }
        }

        private IEnumerable<ActionTableValue> ReadActionsByCreatedAtIndex(TransactionOperationContext context)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(_actionsSchema, ActionsSchema.ActionsTree);
            
            foreach (var it in table.SeekForwardFrom(_actionsSchema.Indexes[ByCreatedAt], Slices.BeforeAllKeys))
            {
                foreach (var holder in it.Results)
                {
                    yield return Read(context, holder.Reader);
                }
            }
        }

        public IDisposable ReadPostponedActions(out IEnumerable<ActionTableValue> actions, DateTime cutoff)
        {
            using (var scope = new DisposeableScope())
            {
                TransactionOperationContext context;

                scope.EnsureDispose(_contextPool.AllocateOperationContext(out context));
                scope.EnsureDispose(context.OpenReadTransaction());

                actions = ReadPostponedActionsByPostponedUntilIndex(context, cutoff);

                return scope.Delay();
            }
        }

        private IEnumerable<ActionTableValue> ReadPostponedActionsByPostponedUntilIndex(TransactionOperationContext context, DateTime cutoff)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(_actionsSchema, ActionsSchema.ActionsTree);

            foreach (var it in table.SeekForwardFrom(_actionsSchema.Indexes[ByPostponedUntil], Slices.BeforeAllKeys))
            {
                foreach (var holder in it.Results)
                {
                    var action = Read(context, holder.Reader);

                    if (action.PostponedUntil == null)
                        continue;

                    if (action.PostponedUntil > cutoff)
                        break;

                    yield return action;
                }
            }
        }

        private ActionTableValue Get(string id, TransactionOperationContext context, RavenTransaction tx)
        {
            var table = tx.InnerTransaction.OpenTable(_actionsSchema, ActionsSchema.ActionsTree);

            Slice slice;
            using (Slice.From(tx.InnerTransaction.Allocator, id, out slice))
            {
                TableValueReader tvr;
                if (table.ReadByKey(slice, out tvr) == false)
                    return null;

                return Read(context, tvr);
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

        public long GetAlertCount()
        {
            var count = 0;

            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                foreach (var action in ReadActionsByCreatedAtIndex(context))
                {
                    object type;
                    if (action.Json.TryGetMember(nameof(Action.Type), out type) == false)
                        throw new InvalidOperationException($"Could not find action type. Action: {action}");

                    var typeLsv = (LazyStringValue) type;
                    
                    if (typeLsv.CompareTo(ActionType.AlertRaised.ToString()) == 0)
                        count++;
                }
            }

            return count;
        }

        private ActionTableValue Read(JsonOperationContext context, TableValueReader reader)
        {
            int size;
            
            var createdAt = new DateTime(Bits.SwapBytes(*(long*)reader.Read(ActionsSchema.ActionsTable.CreatedAtIndex, out size)));

            var postponeUntilTicks = *(long*)reader.Read(ActionsSchema.ActionsTable.PostponedUntilIndex, out size);

            DateTime? postponedUntil = null;
            if (postponeUntilTicks != _postponeDateNotSpecified)
                postponedUntil = new DateTime(Bits.SwapBytes(postponeUntilTicks));

            var jsonPtr = reader.Read(ActionsSchema.ActionsTable.JsonIndex, out size);

            return new ActionTableValue
            {
                CreatedAt = createdAt,
                PostponedUntil = postponedUntil,
                Json = new BlittableJsonReaderObject(jsonPtr, size, context)
            };
        }

        public void ChangePostponeDate(string id, DateTime? postponeUntil)
        {
            TransactionOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                var item = Get(id, context, tx);

                if (item == null)
                    return;

                Store(context.GetDiscardableLazyString(id), item.CreatedAt, postponeUntil, item.Json, tx);

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
                public const int PostponedUntilIndex = 2;
                public const int JsonIndex = 3;
#pragma warning restore 169
            }
        }
    }
}