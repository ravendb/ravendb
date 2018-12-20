using System;
using System.IO;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands
{
    public abstract class CompareExchangeCommandBase : CommandBase
    {
        public string Key;
        public string Database;
        private string _actualKey;

        protected string ActualKey => _actualKey ?? (_actualKey = GetActualKey(Database, Key));

        public long Index;
        [JsonDeserializationIgnore]
        public JsonOperationContext ContextToWriteResult;

        public static string GetActualKey(string database, string key)
        {
            return (database + "/" + key).ToLowerInvariant();
        }

        protected CompareExchangeCommandBase() { }

        protected CompareExchangeCommandBase(string database, string key, long index, JsonOperationContext context)
        {
            if (string.IsNullOrEmpty(key))
                throw new ArgumentNullException(nameof(key), "The key argument must have value");
            if (string.IsNullOrEmpty(database))
                throw new ArgumentNullException(nameof(database), "The database argument must have value");
            if (index < 0)
                throw new InvalidDataException("Index must be a non-negative number");

            Key = key;
            Index = index;
            Database = database;
            ContextToWriteResult = context;
        }

        public abstract (long Index, object Value) Execute(TransactionOperationContext context, Table items, long index);

        public unsafe bool Validate(TransactionOperationContext context, Table items, long index, out long currentIndex)
        {
            currentIndex = -1;
            using (Slice.From(context.Allocator, ActualKey, out Slice keySlice))
            {
                if (items.ReadByKey(keySlice, out var reader))
                {
                    currentIndex = *(long*)reader.Read((int)ClusterStateMachine.UniqueItems.Index, out var _);
                    return Index == currentIndex;
                }
            }
            return index == 0;
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var json = base.ToJson(context);
            json[nameof(Key)] = Key;
            json[nameof(Index)] = Index;
            json[nameof(Database)] = Database;
            return json;
        }

        public override object FromRemote(object remoteResult)
        {
            return JsonDeserializationCluster.CompareExchangeResult((BlittableJsonReaderObject)remoteResult);
        }

        public class CompareExchangeResult : IDynamicJsonValueConvertible
        {
            public long Index;
            public object Value;
            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Index)] = Index,
                    [nameof(Value)] = Value
                };
            }
        }

        public static object ConvertResult(JsonOperationContext ctx, object result)
        {
            if (result is ValueTuple<long, object> tuple)
            {
                if (tuple.Item2 is BlittableJsonReaderObject value)
                {
                    return new CompareExchangeResult
                    {
                        Index = tuple.Item1,
                        Value = ctx.ReadObject(value, "cmpXchg result clone")
                    };
                }

                return new CompareExchangeResult
                {
                    Index = tuple.Item1,
                    Value = tuple.Item2
                };
            }
            throw new RachisApplyException("Unable to convert result type: " + result?.GetType()?.FullName + ", " + result);
        }
    }

    public class RemoveCompareExchangeCommand : CompareExchangeCommandBase
    {
        public RemoveCompareExchangeCommand() { }
        public RemoveCompareExchangeCommand(string key, string database, long index, JsonOperationContext contextToReturnResult) : base(key, database, index, contextToReturnResult) { }

        public override unsafe (long Index, object Value) Execute(TransactionOperationContext context, Table items, long index)
        {
            using (Slice.From(context.Allocator, ActualKey, out Slice keySlice))
            {
                if (items.ReadByKey(keySlice, out var reader))
                {
                    var itemIndex = *(long*)reader.Read((int)ClusterStateMachine.UniqueItems.Index, out var _);
                    var storeValue = reader.Read((int)ClusterStateMachine.UniqueItems.Value, out var size);
                    var result = new BlittableJsonReaderObject(storeValue, size, context);
                    if (Index == itemIndex)
                    {
                        result = result.Clone(context);
                        items.Delete(reader.Id);
                        return (index, result);
                    }
                    return (itemIndex, result);
                }
            }
            return (index, null);
        }
    }

    public class AddOrUpdateCompareExchangeCommand : CompareExchangeCommandBase
    {
        public BlittableJsonReaderObject Value;

        public AddOrUpdateCompareExchangeCommand() { }

        public AddOrUpdateCompareExchangeCommand(string database, string key, BlittableJsonReaderObject value, long index, JsonOperationContext contextToReturnResult) 
            : base(database, key, index, contextToReturnResult)
        {
            Value = value;
        }

        public override unsafe (long Index, object Value) Execute(TransactionOperationContext context, Table items, long index)
        {
            // We have to clone the Value because we might have gotten this command from another node
            // and it was serialized. In that case, it is an _internal_ object, not a full document,
            // so we have to clone it to get it into a standalone mode.
            Value = Value.Clone(context);
            using (Slice.From(context.Allocator, ActualKey, out Slice keySlice))
            using (items.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(keySlice.Content.Ptr, keySlice.Size);
                tvb.Add(index);
                tvb.Add(Value.BasePointer, Value.Size);

                if (items.ReadByKey(keySlice, out var reader))
                {
                    var itemIndex = *(long*)reader.Read((int)ClusterStateMachine.UniqueItems.Index, out var _);
                    if (Index == itemIndex)
                    {
                        items.Update(reader.Id, tvb);
                    }
                    else
                    {
                        // concurrency violation, so we return the current value
                        return (itemIndex, new BlittableJsonReaderObject(reader.Read((int)ClusterStateMachine.UniqueItems.Value, out var size), size, context));
                    }
                }
                else
                {
                    items.Set(tvb);
                }
            }
            return (index, Value);
        }
        
        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var json = base.ToJson(context);
            json[nameof(Value)] = Value;
            return json;
        }
    }
}
