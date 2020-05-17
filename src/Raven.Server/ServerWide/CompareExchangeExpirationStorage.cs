using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Client;
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

        private static IEnumerable<(Slice keySlice, long expiredTicks, Slice ticksSlice)> GetExpiredValues(ClusterOperationContext context, long currentTicks)
        {
            var expirationTree = context.Transaction.InnerTransaction.ReadTree(CompareExchangeByExpiration);
            using (var it = expirationTree.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    yield break;

                do
                {
                    var entryTicks = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                    if (entryTicks > currentTicks)
                        yield break;

                    var ticksAsSlice = it.CurrentKey.Clone(context.Transaction.InnerTransaction.Allocator);

                    using (var multiIt = expirationTree.MultiRead(it.CurrentKey))
                    {
                        if (multiIt.Seek(Slices.BeforeAllKeys))
                        {
                            do
                            {
                                Slice clonedId = multiIt.CurrentKey.Clone(context.Transaction.InnerTransaction.Allocator);

                                yield return (clonedId, entryTicks, ticksAsSlice);
                            } while (multiIt.MoveNext());
                        }
                    }

                } while (it.MoveNext());
            }
        }

        public static bool HasExpiredMetadata(BlittableJsonReaderObject value, out long ticks, Slice keySlice, string storageKey = null)
        {
            if (value.TryGetMember(Constants.Documents.Metadata.Key, out var metadata))
            {
                if (metadata is BlittableJsonReaderObject bjro && bjro.TryGet(Constants.Documents.Metadata.Expires, out object obj))
                {
                    if (obj is LazyStringValue expirationDate)
                    {
                        if (DateTime.TryParseExact(expirationDate, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime date) == false)
                        {
                            storageKey ??= keySlice.ToString();

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
                        storageKey ??= keySlice.ToString();
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
            // we have to use a dictionary to remove from expired multi tree, because there is a chance that not all keys for certain ticks will be returned in single delete iteration
            var expired = new Dictionary<Slice, List<Slice>>();

            foreach ((Slice keySlice, long expiredTicks, Slice ticksSlice) in GetExpiredValues(context, ticks))
            {
                if (take-- <= 0)
                {
                    CleanExpired(context, expired);
                    return true;
                }

                if (expired.TryGetValue(ticksSlice, out List<Slice> list) == false)
                {
                    list = new List<Slice> { keySlice };
                }
                else
                {
                    list.Add(keySlice);
                }

                expired[ticksSlice] = list;

                if (items.ReadByKey(keySlice, out var reader) == false)
                    continue;

                var storeValue = reader.Read((int)ClusterStateMachine.CompareExchangeTable.Value, out var size);
                using var result = new BlittableJsonReaderObject(storeValue, size, context);

                if (HasExpiredMetadata(result, out long currentTicks, keySlice, storageKey: null) == false)
                    continue;

                if (currentTicks > expiredTicks)
                    continue;

                items.Delete(reader.Id);
            }

            CleanExpired(context, expired);
            return false;
        }

        private static void CleanExpired(ClusterOperationContext context, Dictionary<Slice, List<Slice>> expired)
        {
            if (expired.Count == 0)
                return;

            var expirationTree = context.Transaction.InnerTransaction.ReadTree(CompareExchangeByExpiration);
            foreach (var pair in expired)
            {
                foreach (var ids in pair.Value)
                {
                    expirationTree.MultiDelete(pair.Key, ids);
                }
            }
        }
    }
}
