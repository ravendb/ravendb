using System;
using System.IO;
using System.Text;
using Raven.Client.Exceptions;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands
{
    public abstract class CompareExchangeCommandBase : CommandBase
    {
        public string Key;
        public string Database;
        private string _actualKey;
        public bool FromBackup;

        protected string ActualKey => _actualKey ?? (_actualKey = GetActualKey(Database, Key));

        public long Index;
        [JsonDeserializationIgnore]
        public JsonOperationContext ContextToWriteResult;

        public static string GetActualKey(string database, string key)
        {
            return (database + "/" + key).ToLowerInvariant();
        }

        protected CompareExchangeCommandBase() { }

        protected CompareExchangeCommandBase(string database, string key, long index, JsonOperationContext context, string uniqueRequestId, bool fromBackup) : base(uniqueRequestId)
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
            FromBackup = fromBackup;
        }

        public abstract CompareExchangeResult Execute(TransactionOperationContext context, Table items, long index);

        public static unsafe void GetKeyAndPrefixIndexSlices(
            ByteStringContext allocator, string db, string key, long index,
            out (ByteString Buffer, ByteStringContext<ByteStringMemoryCache>.InternalScope Scope) finalKey,
            out (ByteString Buffer, ByteStringContext<ByteStringMemoryCache>.InternalScope Scope) finalIndex)
        {
            var reservedSpace = Encoding.UTF8.GetMaxByteCount(db.Length + key.Length) + 1;  // length of ActualKey 'db/key'
            var keyScope = allocator.Allocate(reservedSpace, out ByteString keyBuffer);
            var indexScope = allocator.Allocate(Encoding.UTF8.GetMaxByteCount(db.Length) + sizeof(long) + 1, out ByteString indexBuffer); // length of 'db/[index]'
            fixed (char* pDb = db, pKey = key)
            {
                var len = Encoding.UTF8.GetBytes(pDb, db.Length, keyBuffer.Ptr, keyBuffer.Length);

                keyBuffer.Ptr[len++] = (byte)'/';
                len += Encoding.UTF8.GetBytes(pKey, key.Length, keyBuffer.Ptr + len, keyBuffer.Length - len);
                keyBuffer.Truncate(len);

                allocator.ToLowerCase(ref keyBuffer);

                len = Encoding.UTF8.GetBytes(pDb, db.Length, indexBuffer.Ptr, indexBuffer.Length);
                indexBuffer.Ptr[len++] = (byte)'/';
                indexBuffer.Truncate(len);
                allocator.ToLowerCase(ref indexBuffer);
                indexBuffer.Truncate(indexBuffer.Length + sizeof(long));

                *(long*)(indexBuffer.Ptr + len) = Bits.SwapBytes(index);

                finalIndex = (indexBuffer, indexScope);
                finalKey = (keyBuffer, keyScope);
            }
        }

        public static unsafe ByteStringContext<ByteStringMemoryCache>.InternalScope GetPrefixIndexSlices(
           ByteStringContext allocator, string db, long index,
            out ByteString finalIndex)
        {
            var indexScope = allocator.Allocate(Encoding.UTF8.GetMaxByteCount(db.Length) + sizeof(long) + 1, out ByteString indexBuffer);
            fixed (char* pDb = db)
            {
                var len = Encoding.UTF8.GetBytes(pDb, db.Length, indexBuffer.Ptr, indexBuffer.Length);
                indexBuffer.Ptr[len++] = (byte)'/';
                indexBuffer.Truncate(len);
                allocator.ToLowerCase(ref indexBuffer);
                indexBuffer.Truncate(indexBuffer.Length + sizeof(long));

                *(long*)(indexBuffer.Ptr + len) = Bits.SwapBytes(index);

                finalIndex = indexBuffer;

                return indexScope;
            }
        }

        public static unsafe void GetDbPrefixAndLastSlices(
            ByteStringContext allocator, string db,
            out (Slice Slice, ByteStringContext.ExternalScope Scope) prefix,
            out (Slice Slice, ByteStringContext.ExternalScope Scope) last)
        {
            var reservedSpace = Encoding.UTF8.GetMaxByteCount(db.Length) + 1;

            allocator.Allocate(reservedSpace, out var prefixBuffer); // db + '/'
            allocator.Allocate(reservedSpace, out var lastBuffer); // db + (char)('/'+1)

            fixed (char* pDb = db)
            {
                var len = Encoding.UTF8.GetBytes(pDb, db.Length, prefixBuffer.Ptr, prefixBuffer.Length);

                prefixBuffer.Ptr[len] = (byte)'/';
                prefixBuffer.Truncate(len + 1);

                allocator.ToLowerCase(ref prefixBuffer);

                prefixBuffer.CopyTo(lastBuffer.Ptr);

                lastBuffer.Ptr[len++] = '/' + 1;
                lastBuffer.Truncate(len);
            }

            prefix.Scope = Slice.External(allocator, prefixBuffer, prefixBuffer.Length, out prefix.Slice);
            last.Scope = Slice.External(allocator, lastBuffer, lastBuffer.Length, out last.Slice);

        }

        public unsafe bool Validate(TransactionOperationContext context, Table items, long index, out long currentIndex)
        {
            currentIndex = -1;
            using (Slice.From(context.Allocator, ActualKey, out Slice keySlice))
            {
                if (items.ReadByKey(keySlice, out var reader))
                {
                    currentIndex = *(long*)reader.Read((int)ClusterStateMachine.CompareExchangeTable.Index, out var _);
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
            json[nameof(FromBackup)] = FromBackup;
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
            if (result is CompareExchangeResult tuple)
            {
                if (tuple.Value is BlittableJsonReaderObject value)
                {
                    return new CompareExchangeResult
                    {
                        Index = tuple.Index,
                        Value = ctx.ReadObject(value, "cmpXchg result clone")
                    };
                }

                return tuple;
            }

            if (result is BlittableJsonReaderObject blittable)
            {
                var converted = JsonDeserializationCluster.CompareExchangeResult(blittable);
                if (converted.Value is BlittableJsonReaderObject val)
                {
                    converted.Value = ctx.ReadObject(val, "cmpXchg result clone");
                }

                return converted;
            }

            throw new RachisApplyException("Unable to convert result type: " + result?.GetType()?.FullName + ", " + result);
        }
    }

    public class RemoveCompareExchangeCommand : CompareExchangeCommandBase
    {
        public RemoveCompareExchangeCommand() { }

        public RemoveCompareExchangeCommand(string database, string key, long index, JsonOperationContext contextToReturnResult, string uniqueRequestId, bool fromBackup = false) : base(database, key,
            index, contextToReturnResult, uniqueRequestId, fromBackup)
        {
        }

        public override unsafe CompareExchangeResult Execute(TransactionOperationContext context, Table items, long index)
        {
            using (Slice.From(context.Allocator, ActualKey, out Slice keySlice))
            {
                if (items.ReadByKey(keySlice, out var reader))
                {
                    if (FromBackup)
                    {
                        items.Delete(reader.Id);
                        return new CompareExchangeResult
                        {
                            Index = index,
                            Value = null
                        };
                    }
                    var itemIndex = *(long*)reader.Read((int)ClusterStateMachine.CompareExchangeTable.Index, out var _);
                    var storeValue = reader.Read((int)ClusterStateMachine.CompareExchangeTable.Value, out var size);
                    var result = new BlittableJsonReaderObject(storeValue, size, context);

                    if (Index == itemIndex)
                    {
                        result = result.Clone(context);
                        items.Delete(reader.Id);
                        WriteCompareExchangeTombstone(context, index);
                        return new CompareExchangeResult
                        {
                            Index = index,
                            Value = result
                        };
                    }
                    return new CompareExchangeResult
                    {
                        Index = itemIndex,
                        Value = result
                    };
                }
                // if we ever decide to make replication of compare exchange, then we should write the compare exchange tombstones to schema on restore
            }
            return new CompareExchangeResult
            {
                Index = index,
                Value = null
            };
        }

        private unsafe void WriteCompareExchangeTombstone(TransactionOperationContext context, long index)
        {
            GetKeyAndPrefixIndexSlices(context.Allocator, Database, Key, index, out var keyTuple, out var indexTuple);
            var tombstoneItems = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.CompareExchangeTombstoneSchema, ClusterStateMachine.CompareExchangeTombstones);
            using (keyTuple.Scope)
            using (indexTuple.Scope)
            using (Slice.External(context.Allocator, keyTuple.Buffer.Ptr, keyTuple.Buffer.Length, out var keySlice))
            using (Slice.External(context.Allocator, indexTuple.Buffer.Ptr, indexTuple.Buffer.Length, out var prefixIndexSlice))
            using (tombstoneItems.Allocate(out var tvb))
            {
                tvb.Add(keySlice.Content.Ptr, keySlice.Size);
                tvb.Add(index);
                tvb.Add(prefixIndexSlice);
                tombstoneItems.Set(tvb);
            }
        }
    }

    public class AddOrUpdateCompareExchangeCommand : CompareExchangeCommandBase
    {
        private static readonly UTF8Encoding Encoding = new UTF8Encoding();

        internal const int MaxNumberOfCompareExchangeKeyBytes = 512;

        public BlittableJsonReaderObject Value;

        public AddOrUpdateCompareExchangeCommand() { }

        public AddOrUpdateCompareExchangeCommand(string database, string key, BlittableJsonReaderObject value, long index, JsonOperationContext contextToReturnResult, string uniqueRequestId, bool fromBackup = false)
            : base(database, key, index, contextToReturnResult, uniqueRequestId, fromBackup)
        {
            if (key.Length > MaxNumberOfCompareExchangeKeyBytes || Encoding.GetByteCount(key) > MaxNumberOfCompareExchangeKeyBytes)
                ThrowCompareExchangeKeyTooBig(key);

            Value = value;
        }

        public override unsafe CompareExchangeResult Execute(TransactionOperationContext context, Table items, long index)
        {
            // We have to clone the Value because we might have gotten this command from another node
            // and it was serialized. In that case, it is an _internal_ object, not a full document,
            // so we have to clone it to get it into a standalone mode.
            Value = Value.Clone(context);
            GetKeyAndPrefixIndexSlices(context.Allocator, Database, Key, index, out var keyTuple, out var indexTuple);

            using (keyTuple.Scope)
            using (indexTuple.Scope)
            using (Slice.External(context.Allocator, keyTuple.Buffer.Ptr, keyTuple.Buffer.Length, out var keySlice))
            using (Slice.External(context.Allocator, indexTuple.Buffer.Ptr, indexTuple.Buffer.Length, out var prefixIndexSlice))
            using (items.Allocate(out var tvb))
            {
                tvb.Add(keySlice.Content.Ptr, keySlice.Size);
                tvb.Add(index);
                tvb.Add(Value.BasePointer, Value.Size);
                tvb.Add(prefixIndexSlice);

                if (items.ReadByKey(keySlice, out var reader))
                {
                    var itemIndex = *(long*)reader.Read((int)ClusterStateMachine.CompareExchangeTable.Index, out var _);
                    if (Index == itemIndex || (FromBackup && Index == 0))
                    {
                        items.Update(reader.Id, tvb);
                    }
                    else
                    {
                        // concurrency violation, so we return the current value
                        return new CompareExchangeResult
                        {
                            Index = itemIndex,
                            Value = new BlittableJsonReaderObject(reader.Read((int)ClusterStateMachine.CompareExchangeTable.Value, out var size), size, context)
                        };
                    }
                }
                else
                {
                    items.Set(tvb);
                }
            }
            return new CompareExchangeResult
            {
                Index = index,
                Value = Value
            };
        }

        public override DynamicJsonValue ToJson(JsonOperationContext context)
        {
            var json = base.ToJson(context);
            json[nameof(Value)] = Value;
            return json;
        }

        private static void ThrowCompareExchangeKeyTooBig(string str)
        {
            throw new CompareExchangeKeyTooBigException(
                $"Compare Exchange key cannot exceed {MaxNumberOfCompareExchangeKeyBytes} bytes, " +
                $"but the key was {Encoding.GetByteCount(str)} bytes. The invalid key is '{str}'. Parameter '{nameof(str)}'");
    }
    }
}
