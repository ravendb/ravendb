using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Client;
using Raven.Client.Util;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide
{
    public class CompareExchangeExpirationStorage
    {
        public static string CompareExchangeByExpiration = "CompareExchangeByExpiration";
        private static Dictionary<Slice, List<Slice>> _expired;

        public static unsafe void Put(ClusterOperationContext context, Slice keySlice, long ticks)
        {
            var ticksBigEndian = Bits.SwapBytes(ticks);
            using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
            {
                var tree = context.Transaction.InnerTransaction.ReadTree(CompareExchangeByExpiration);
                tree.MultiAdd(ticksSlice, keySlice);
            }
        }

        public static bool HasExpired(TransactionOperationContext context, long currentTicks)
        {
            var expirationTree = context.Transaction.InnerTransaction.ReadTree(CompareExchangeByExpiration);
            using (var it = expirationTree.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    return false;

                var entryTicks = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                if (entryTicks > currentTicks)
                    return false;
            }

            return true;
        }

        public static IEnumerable<(Slice keySlice, long expiredTicks)> GetExpiredValues(ClusterOperationContext context, long currentTicks)
        {
            var expirationTree = context.Transaction.InnerTransaction.ReadTree(CompareExchangeByExpiration);
            using (var it = expirationTree.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                {
                    yield break;
                }
                do
                {
                    var entryTicks = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                    if (entryTicks > currentTicks)
                    {
                        yield break;
                    }

                    var ticksAsSlice = it.CurrentKey.Clone(context.Transaction.InnerTransaction.Allocator);

                    var expiredDocs = new List<Slice>();
                    using (var multiIt = expirationTree.MultiRead(it.CurrentKey))
                    {
                        if (multiIt.Seek(Slices.BeforeAllKeys))
                        {
                            do
                            {
                                Slice clonedId = multiIt.CurrentKey.Clone(context.Transaction.InnerTransaction.Allocator);
                                expiredDocs.Add(clonedId);

                                yield return (clonedId, entryTicks);
                            } while (multiIt.MoveNext());
                        }
                    }

                    if (expiredDocs.Count > 0)
                        _expired.Add(ticksAsSlice, expiredDocs);

                } while (it.MoveNext());
            }
        }

        public static bool HasExpiredMetadata(string storageKey, BlittableJsonReaderObject value, out long ticks)
        {
            if (value.TryGetMember(Constants.Documents.Metadata.Key, out var metadata))
            {
                if (metadata is BlittableJsonReaderObject bjro && bjro.TryGet(Constants.Documents.Metadata.Expires, out object obj))
                {
                    if (obj is LazyStringValue expirationDate)
                    {
                        if (DateTime.TryParseExact(expirationDate, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime date) == false)
                        {
                            var inner = new InvalidOperationException(
                                $"The expiration date format for compare exchange '{CompareExchangeKey.SplitStorageKey(storageKey).Key}' is not valid: '{expirationDate}'. Use the following format: {DateTime.UtcNow:O}");
                            throw new RachisApplyException("Could not apply command.", inner);
                        }
                        var expiry = date.ToUniversalTime();
                        ticks = expiry.Ticks;
                        return true;
                    }
                    else
                    {
                        var inner = new InvalidOperationException($"The type of {Constants.Documents.Metadata.Expires} metadata for compare exchange '{CompareExchangeKey.SplitStorageKey(storageKey).Key}' is not valid. Use the following type: {nameof(DateTime)}");
                        throw new RachisApplyException("Could not apply command.", inner);
                    }
                }
            }

            ticks = default;
            return false;
        }

        public static unsafe bool DeleteExpiredCompareExchange(ClusterOperationContext context, Table items, long ticks, long take = long.MaxValue)
        {
            using (ExpiredCleaner(context))
            {
                foreach (var tuple in GetExpiredValues(context, ticks))
                {
                    if (take-- <= 0)
                        return true;

                    if (items.ReadByKey(tuple.keySlice, out var reader) == false)
                        continue;

                    var storeValue = reader.Read((int)ClusterStateMachine.CompareExchangeTable.Value, out var size);
                    using var result = new BlittableJsonReaderObject(storeValue, size, context);

                    if (HasExpiredMetadata(tuple.keySlice.ToString(), result, out long currentTicks) == false)
                        continue;
                    if (currentTicks > tuple.expiredTicks)
                        continue;

                    items.Delete(reader.Id);
                }

                return false;
            }
        }

        public static IDisposable ExpiredCleaner(ClusterOperationContext context)
        {
            _expired = new Dictionary<Slice, List<Slice>>();

            return new DisposableAction(() =>
            {

                ClearExpired(context);
                _expired = null;
            });
        }

        private static void ClearExpired(ClusterOperationContext context)
        {
            if (_expired.Count == 0)
                return;

            var expirationTree = context.Transaction.InnerTransaction.ReadTree(CompareExchangeByExpiration);
            foreach (var pair in _expired)
            {
                foreach (var ids in pair.Value)
                {
                    expirationTree.MultiDelete(pair.Key, ids);
                }
            }
        }
        private static unsafe long ReadDateTicks2(Slice ticksSlice)
        {
            var index = *(long*)ticksSlice.Content.Ptr;
            return index;
        }
    }
}
