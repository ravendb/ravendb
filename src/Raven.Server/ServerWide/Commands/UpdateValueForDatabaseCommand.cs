using System;
using System.Diagnostics;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands
{
    public abstract class UpdateValueForDatabaseCommand : CommandBase
    {
        public string DatabaseName { get; set; }
        public abstract string GetItemId();

        public abstract void FillJson(DynamicJsonValue json);

        protected abstract UpdatedValue GetUpdatedValue(long index, RawDatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue);

        public virtual unsafe void Execute(ClusterOperationContext context, Table items, long index, RawDatabaseRecord record, RachisState state, out object result)
        {
            result = null;

            var itemKey = GetItemId();
            UpdatedValue updatedValue;

            using (Slice.From(context.Allocator, itemKey.ToLowerInvariant(), out Slice valueNameLowered))
            {
                BlittableJsonReaderObject itemBlittable = null;
                if (items.ReadByKey(valueNameLowered, out TableValueReader reader))
                {
                    var ptr = reader.Read(2, out int size);
                    itemBlittable = new BlittableJsonReaderObject(ptr, size, context);
                }

                updatedValue = GetUpdatedValue(index, record, context, itemBlittable);
                switch (updatedValue.ActionType)
                {
                    case UpdatedValueActionType.Noop:
                        return;
                    case UpdatedValueActionType.Delete:
                        items.DeleteByKey(valueNameLowered);
                        return;
                    default:
                        // here we get the item key again, in case it was changed (a new entity, etc)
                        itemKey = GetItemId();
                        break;
                }
            }

            Debug.Assert(updatedValue.ActionType == UpdatedValueActionType.Update && updatedValue.Value != null);

            using (Slice.From(context.Allocator, itemKey, out Slice valueName))
            using (Slice.From(context.Allocator, itemKey.ToLowerInvariant(), out Slice valueNameLowered))
            using (updatedValue)
            {
                ClusterStateMachine.UpdateValue(index, items, valueNameLowered, valueName, updatedValue.Value);
            }
        }

        public virtual object GetState()
        {
            return null;
        }

        public static unsafe long GetValue(TableValueReader tvr)
        {
            return *(long*)tvr.Read((int)ClusterStateMachine.IdentitiesTable.Value, out _);
        }

        public static void UpdateTableRow(long index, Table identitiesItems, long value, Slice keySlice, Slice prefixIndexSlice)
        {
            using (identitiesItems.Allocate(out var tvb))
            {
                tvb.Add(keySlice);
                tvb.Add(value);
                tvb.Add(index);
                tvb.Add(prefixIndexSlice);

                identitiesItems.Set(tvb);
            }
        }

        protected UpdateValueForDatabaseCommand() { }

        protected UpdateValueForDatabaseCommand(string databaseName, string uniqueRequestId) : base(uniqueRequestId)
        {
            DatabaseName = databaseName;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var djv = base.ToJson(context);
            djv[nameof(DatabaseName)] = DatabaseName;

            FillJson(djv);

            return djv;
        }

        public static string GetDatabaseNameFromJson(BlittableJsonReaderObject cmd)
        {
            string databaseName = null;
            cmd?.TryGet(nameof(DatabaseName), out databaseName);
            return databaseName;
        }

        protected enum UpdatedValueActionType
        {
            Noop,
            Update,
            Delete
        }

        protected readonly struct UpdatedValue : IDisposable
        {
            public readonly UpdatedValueActionType ActionType;

            public readonly BlittableJsonReaderObject Value;

            public UpdatedValue(UpdatedValueActionType actionType, BlittableJsonReaderObject value)
            {
                ActionType = actionType;
                Value = value;
            }

            public void Dispose()
            {
                Value?.Dispose();
            }
        }
    }
}
