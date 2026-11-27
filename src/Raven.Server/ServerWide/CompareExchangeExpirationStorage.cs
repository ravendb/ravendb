using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide
{
    public sealed class CompareExchangeExpirationStorage
    {
        public static readonly Slice CompareExchangeByExpiration;

        static CompareExchangeExpirationStorage()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, nameof(CompareExchangeByExpiration), out CompareExchangeByExpiration);
            }
        }

        public static unsafe void Put(ClusterOperationContext context, Slice keySlice, long ticks)
        {
            var ticksBigEndian = Bits.SwapBytes(ticks);
            using (Slice.External(context.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
            {
                var tree = context.Transaction.InnerTransaction.ReadTree(CompareExchangeByExpiration);
                tree.MultiAdd(ticksSlice, keySlice);
            }
        }

        public static bool HasExpired(ClusterOperationContext context, long currentTicks)
        {
            var expirationTree = context.Transaction.InnerTransaction.ReadTree(CompareExchangeByExpiration);
            using (var it = expirationTree.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    return false;

                var entryTicks = it.CurrentKey.CreateReader().ReadBigEndianInt64();
                return entryTicks <= currentTicks;
            }
        }

        internal static IEnumerable<(Slice keySlice, long expiredTicks, Slice ticksSlice)> GetExpiredValues(ClusterOperationContext context, long currentTicks)
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

        public static bool TryGetExpires(BlittableJsonReaderObject value, out long ticks)
        {
            ticks = default;
            if (value.TryGetMember(Constants.Documents.Metadata.Key, out var metadata) == false || metadata is not BlittableJsonReaderObject metadataReader)
                return false;
        
            if (metadataReader.TryGet(Constants.Documents.Metadata.Expires, out LazyStringValue expirationDate) == false)
                return false;

            // Apply runs on every node, so it must not use the local time zone: a time without one is UTC (the sending node
            // converts local times before the command is in the log), a time with an offset converts with that offset
            if (DateTime.TryParseExact(expirationDate, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var date) == false)
                throw new FormatException($"{Constants.Documents.Metadata.Expires} should contain date but has {expirationDate}': {value}");

            ticks = date.Ticks;
            return true;
        }

        /// <summary>
        /// Runs on the node that received the request, before the command is in the log: apply reads @expires without a time zone
        /// as UTC, so a local time is converted here, with the time zone of this node, and written back to the value as UTC.
        /// </summary>
        public static BlittableJsonReaderObject ConvertExpiresToUtc(JsonOperationContext context, BlittableJsonReaderObject value, TimeZoneInfo localTimeZone)
        {
            if (value == null ||
                value.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false || metadata == null ||
                metadata.TryGet(Constants.Documents.Metadata.Expires, out LazyStringValue expires) == false || expires == null)
                return value;

            // an invalid date is left as is, apply rejects it
            if (DateTime.TryParseExact(expires, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var date) == false ||
                date.Kind != DateTimeKind.Unspecified)
                return value;

            // GetUtcOffset, not ConvertTimeToUtc: that throws for a time inside a daylight saving gap
            var utc = new DateTime(Math.Clamp(date.Ticks - localTimeZone.GetUtcOffset(date).Ticks, DateTime.MinValue.Ticks, DateTime.MaxValue.Ticks), DateTimeKind.Utc);
            metadata.Modifications = new DynamicJsonValue(metadata) { [Constants.Documents.Metadata.Expires] = utc.GetDefaultRavenFormat(isUtc: true) };
            value.Modifications = new DynamicJsonValue(value) { [Constants.Documents.Metadata.Key] = metadata };
            return context.ReadObject(value, "compare-exchange-value");
        }

        public static unsafe bool DeleteExpiredCompareExchange(ClusterOperationContext context, Table items, long ticks, long take = long.MaxValue)
        {
            // we have to use a dictionary to remove from expired multi tree, because there is a chance that not all keys for certain ticks will be returned in single delete iteration
            var expired = new Dictionary<Slice, List<Slice>>(SliceComparer.Instance);
            List<(Slice Key, long Ticks)> expiresLater = null;

            foreach ((Slice keySlice, long expiredTicks, Slice ticksSlice) in GetExpiredValues(context, ticks))
            {
                if (take-- <= 0)
                {
                    CleanExpired(context, expired);
                    PutExpiresLater(context, expiresLater);
                    return true;
                }

                if (expired.TryGetValue(ticksSlice, out List<Slice> list) == false)
                    expired[ticksSlice] = list = new List<Slice>();

                list.Add(keySlice);

                if (items.ReadByKey(keySlice, out var reader) == false)
                    continue;

                var storeValue = reader.Read((int)ClusterStateMachine.CompareExchangeTable.Value, out var size);
                using var result = new BlittableJsonReaderObject(storeValue, size, context);

                if (TryGetExpires(result, out long currentTicks) == false)
                    continue;

                if (currentTicks > expiredTicks)
                {
                    expiresLater ??= new List<(Slice Key, long Ticks)>();
                    expiresLater.Add((keySlice, currentTicks));
                    continue;
                }

                items.Delete(reader.Id);
            }

            CleanExpired(context, expired);
            PutExpiresLater(context, expiresLater);
            return false;
        }

        private static void PutExpiresLater(ClusterOperationContext context, List<(Slice Key, long Ticks)> expiresLater)
        {
            if (expiresLater == null)
                return;

            foreach (var (key, ticks) in expiresLater)
                Put(context, key, ticks);
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
