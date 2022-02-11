using System.Collections.Generic;
using Raven.Server.ServerWide;
using Sparrow.Binary;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Storage.Schema.Updates.Server
{
    internal class From53000 : ISchemaUpdate
    {
        public int From => 53_000;
        public int To => 53_001;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Server;
        public bool Update(UpdateStep step)
        {
            step.WriteTx.DeleteTree(CompareExchangeExpirationStorage.CompareExchangeByExpiration);
            step.WriteTx.CreateTree(CompareExchangeExpirationStorage.CompareExchangeByExpiration);

            foreach (var (key, value) in GetAllCompareExchange(step.ReadTx))
            {
                if (CompareExchangeExpirationStorage.TryGetExpires(value, out var ticks))
                {
                    Put(step.WriteTx, key, ticks);
                }
            }

            return true;
        }

        public static IEnumerable<(Slice Key, BlittableJsonReaderObject Value)> GetAllCompareExchange(Transaction tx)
        {
            var table = tx.OpenTable(ClusterStateMachine.CompareExchangeSchema, ClusterStateMachine.CompareExchange);
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                foreach (var tvr in table.SeekForwardFrom(ClusterStateMachine.CompareExchangeSchema.Indexes[ClusterStateMachine.CompareExchangeIndex], Slices.BeforeAllKeys, 0))
                {
                    var key = ReadCompareExchangeKey(context, tvr.Result.Reader);
                    var value = ReadCompareExchangeValue(context, tvr.Result.Reader);

                    using (value)
                    using (Slice.From(tx.Allocator, key, out var keySlice))
                    {
                        yield return (keySlice, value);
                    }
                }
            }
        }

        private static unsafe BlittableJsonReaderObject ReadCompareExchangeValue(JsonOperationContext context, TableValueReader reader)
        {
            return new BlittableJsonReaderObject(reader.Read((int)ClusterStateMachine.CompareExchangeTable.Value, out var size), size, context);
        }

        private static unsafe LazyStringValue ReadCompareExchangeKey(JsonOperationContext context, TableValueReader reader)
        {
            var ptr = reader.Read((int)ClusterStateMachine.CompareExchangeTable.Key, out var size);
            return context.AllocateStringValue(null, ptr, size);
        }

        private static unsafe void Put(Transaction tx, Slice keySlice, long ticks)
        {
            var ticksBigEndian = Bits.SwapBytes(ticks);
            using (Slice.External(tx.Allocator, (byte*)&ticksBigEndian, sizeof(long), out Slice ticksSlice))
            {
                var tree = tx.ReadTree(CompareExchangeExpirationStorage.CompareExchangeByExpiration);
                tree.MultiAdd(ticksSlice, keySlice);
            }
        }
    }
}
