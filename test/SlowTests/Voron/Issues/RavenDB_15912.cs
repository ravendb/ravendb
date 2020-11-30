using System;
using System.Linq;
using FastTests.Voron;
using FastTests.Voron.Tables;
using Sparrow.Binary;
using Voron;
using Voron.Data.Tables;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_15912 : StorageTest
    {
        public RavenDB_15912(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public unsafe void OnDataMoveShouldForgetOldCompressionIds()
        {
            var random = new Random(357);
            using (var tx = Env.WriteTransaction())
            {
                Slice.From(tx.Allocator, "Compression", out var etagIndexName);
                var fixedSizedIndex = new TableSchema.FixedSizeSchemaIndexDef
                {
                    Name = etagIndexName,
                    IsGlobal = true,
                    StartIndex = 1,
                };

                var tableSchema = new TableSchema()
                    .CompressValues(fixedSizedIndex, true)
                    .DefineFixedSizeIndex(fixedSizedIndex)
                    .DefineKey(new TableSchema.SchemaIndexDef
                    {
                        StartIndex = 0,
                        Count = 1,
                    });

                tableSchema.Create(tx, "Items", 16);
                var itemsTable = tx.OpenTable(tableSchema, "Items");

                
                 var rnd = Enumerable.Range(1, 32)
                     .Select(i => Compression.RandomString(random, 123))
                     .ToList();

                for (int i = 0; i < 32*1024; i++)
                {
                    using (itemsTable.Allocate(out TableValueBuilder builder))
                    using (Slice.From(tx.Allocator, "val" + i, out var key))
                    using (Slice.From(tx.Allocator, rnd[i%rnd.Count], out var val))
                    {
                        builder.Add(key);
                        builder.Add(Bits.SwapBytes((long)i));
                        builder.Add(val);
                        itemsTable.Insert(builder);
                    }
                }

                AssertRemainingValues();

                for (int i = 32*1024 - 1; i >= 0; i--)
                {
                    using (Slice.From(tx.Allocator, "val" + i, out var key))
                    {
                        if (i % 10 != 0)
                            itemsTable.DeleteByKey(key);
                    }
                }

                AssertRemainingValues(validate: true);

                void AssertRemainingValues(bool validate = false)
                {
                    foreach (var reader in itemsTable.SeekBackwardFrom(fixedSizedIndex, long.MaxValue))
                    {
                        var p = reader.Reader.Read(1, out var sizeOfInt);
                        Assert.Equal(sizeOfInt, sizeof(long));
                        var i = Bits.SwapBytes(*(long*)p);

                        var keyPtr = reader.Reader.Read(0, out var keySize);
                        using (Slice.From(tx.Allocator, keyPtr, keySize, out var currentKey))
                        using (Slice.From(tx.Allocator, "val" + i, out var expectedKey))
                        {
                            Assert.True(SliceStructComparer.Instance.Equals(currentKey, expectedKey));
                        }

                        if (validate)
                        {
                            if (i % 10 != 0)
                                Assert.True(false,$"Oops {i}");
                        }
                    }
                }
            }
        }
    }
}
