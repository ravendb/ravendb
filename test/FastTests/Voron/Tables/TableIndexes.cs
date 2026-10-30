using Sparrow.Binary;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Voron.Exceptions;
using Xunit;

namespace FastTests.Voron.Tables
{
    public class TableIndexes(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenFact(RavenTestCategory.Voron)]
        public void Insert_same_value_to_fixed_sized_index_throws()
        {
            using (var tx = Env.WriteTransaction())
            {
                Slice.From(tx.Allocator, "EtagIndexName", out var etagIndexName);
                var fixedSizedIndex = new TableSchema.FixedSizeKeyIndexDef
                {
                    Name = etagIndexName,
                    IsGlobal = true,
                    StartIndex = 1,
                };

                var tableSchema = new TableSchema()
                    .DefineFixedSizeIndex(fixedSizedIndex)
                    .DefineKey(new TableSchema.IndexDef
                    {
                        StartIndex = 0,
                        Count = 1,
                    });

                tableSchema.Create(tx, "Items", 16);
                var itemsTable = tx.OpenTable(tableSchema, "Items");
                const long number = 1L;

                using (itemsTable.Allocate(out TableValueBuilder builder))
                using (Slice.From(tx.Allocator, "val1", out var key))
                {
                    builder.Add(key);
                    builder.Add(Bits.SwapBytes(number));
                    itemsTable.Set(builder);
                }

                using (itemsTable.Allocate(out TableValueBuilder builder))
                using (Slice.From(tx.Allocator, "val2", out var key))
                {
                    builder.Add(key);
                    builder.Add(Bits.SwapBytes(number));

                    var exception = Assert.Throws<VoronErrorException>(() => itemsTable.Set(builder));
                    Assert.True(exception.Message.StartsWith("Attempt to add duplicate value"));
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Update_same_value_to_fixed_sized_index_throws()
        {
            using (var tx = Env.WriteTransaction())
            {
                Slice.From(tx.Allocator, "EtagIndexName", out var etagIndexName);
                var fixedSizedIndex = new TableSchema.FixedSizeKeyIndexDef
                {
                    Name = etagIndexName,
                    IsGlobal = true,
                    StartIndex = 1,
                };

                var tableSchema = new TableSchema()
                    .DefineFixedSizeIndex(fixedSizedIndex)
                    .DefineKey(new TableSchema.IndexDef
                    {
                        StartIndex = 0,
                        Count = 1,
                    });

                tableSchema.Create(tx, "Items", 16);
                var itemsTable = tx.OpenTable(tableSchema, "Items");
                const long number1 = 1L;
                const long number2 = 2L;
                const long number3 = 3L;

                using (itemsTable.Allocate(out TableValueBuilder builder))
                using (Slice.From(tx.Allocator, "val1", out var key))
                {
                    builder.Add(key);
                    builder.Add(Bits.SwapBytes(number1));
                    itemsTable.Set(builder);
                }

                using (itemsTable.Allocate(out TableValueBuilder builder))
                using (Slice.From(tx.Allocator, "val2", out var key))
                {
                    builder.Add(key);
                    builder.Add(Bits.SwapBytes(number2));
                    itemsTable.Set(builder);
                }

                using (itemsTable.Allocate(out TableValueBuilder builder))
                using (Slice.From(tx.Allocator, "val1", out var key))
                {
                    builder.Add(key);
                    builder.Add(Bits.SwapBytes(number3));
                    itemsTable.Set(builder);
                }

                using (itemsTable.Allocate(out TableValueBuilder builder))
                using (Slice.From(tx.Allocator, "val2", out var key))
                {
                    builder.Add(key);
                    builder.Add(Bits.SwapBytes(number3));

                    var exception = Assert.Throws<VoronErrorException>(() => itemsTable.Set(builder));
                    Assert.True(exception.Message.StartsWith("Attempt to add duplicate value"));
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Insert_same_value_to_index_deosnt_throw()
        {
            using (var tx = Env.WriteTransaction())
            {
                Slice.From(tx.Allocator, "EtagIndexName", out var etagIndexName);
                var index = new TableSchema.IndexDef
                {
                    Name = etagIndexName,
                    StartIndex = 0,
                    Count = 1
                };

                var tableSchema = new TableSchema()
                    .DefineIndex(index)
                    .DefineKey(new TableSchema.IndexDef
                    {
                        StartIndex = 0,
                        Count = 1,
                    });

                Slice.From(tx.Allocator, "Items", out var items);

                tableSchema.Create(tx, "Items", 16);
                var itemsTable = tx.OpenTable(tableSchema, "Items");
                const long number = 1L;

                using (itemsTable.Allocate(out TableValueBuilder builder))
                using (Slice.From(tx.Allocator, "val1", out var key))
                {
                    builder.Add(key);
                    builder.Add(Bits.SwapBytes(number));
                    itemsTable.Set(builder);
                }

                using (itemsTable.Allocate(out TableValueBuilder builder))
                using (Slice.From(tx.Allocator, "val2", out var key))
                {
                    builder.Add(key);
                    builder.Add(Bits.SwapBytes(number));
                    itemsTable.Set(builder);
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Update_same_value_to_index_doesnt_throw()
        {
            using (var tx = Env.WriteTransaction())
            {
                Slice.From(tx.Allocator, "EtagIndexName", out var etagIndexName);
                var index = new TableSchema.IndexDef
                {
                    Name = etagIndexName,
                    StartIndex = 0,
                    Count = 1
                };

                var tableSchema = new TableSchema()
                    .DefineIndex(index)
                    .DefineKey(new TableSchema.IndexDef
                    {
                        StartIndex = 0,
                        Count = 1,
                    });

                tableSchema.Create(tx, "Items", 16);
                var itemsTable = tx.OpenTable(tableSchema, "Items");
                const long number1 = 1L;
                const long number2 = 2L;
                const long number3 = 3L;

                using (itemsTable.Allocate(out TableValueBuilder builder))
                using (Slice.From(tx.Allocator, "val1", out var key))
                {
                    builder.Add(key);
                    builder.Add(Bits.SwapBytes(number1));
                    itemsTable.Set(builder);
                }

                using (itemsTable.Allocate(out TableValueBuilder builder))
                using (Slice.From(tx.Allocator, "val2", out var key))
                {
                    builder.Add(key);
                    builder.Add(Bits.SwapBytes(number2));
                    itemsTable.Set(builder);
                }

                using (itemsTable.Allocate(out TableValueBuilder builder))
                using (Slice.From(tx.Allocator, "val1", out var key))
                {
                    builder.Add(key);
                    builder.Add(Bits.SwapBytes(number3));
                    itemsTable.Set(builder);
                }

                using (itemsTable.Allocate(out TableValueBuilder builder))
                using (Slice.From(tx.Allocator, "val2", out var key))
                {
                    builder.Add(key);
                    builder.Add(Bits.SwapBytes(number3));
                    itemsTable.Set(builder);
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void delete_by_fixed_sized_index()
        {
            using (var tx = Env.WriteTransaction())
            {
                Slice.From(tx.Allocator, "EtagIndexName", out var etagIndexName);
                var fixedSizedIndex = new TableSchema.FixedSizeKeyIndexDef
                {
                    Name = etagIndexName,
                    IsGlobal = true,
                    StartIndex = 1,
                };

                var tableSchema = new TableSchema()
                    .DefineFixedSizeIndex(fixedSizedIndex)
                    .DefineKey(new TableSchema.IndexDef
                    {
                        StartIndex = 0,
                        Count = 1,
                    });

                tableSchema.Create(tx, "Items", 16);
                var itemsTable = tx.OpenTable(tableSchema, "Items");
                const long number1 = 1L;
                const long number2 = 2L;
                const long number3 = 3L;

                using (itemsTable.Allocate(out TableValueBuilder builder))
                using (Slice.From(tx.Allocator, "val1", out var key))
                {
                    builder.Add(key);
                    builder.Add(Bits.SwapBytes(number1));
                    itemsTable.Set(builder);
                }

                using (itemsTable.Allocate(out TableValueBuilder builder))
                using (Slice.From(tx.Allocator, "val2", out var key))
                {
                    builder.Add(key);
                    builder.Add(Bits.SwapBytes(number2));
                    itemsTable.Set(builder);
                }

                using (itemsTable.Allocate(out TableValueBuilder builder))
                using (Slice.From(tx.Allocator, "val3", out var key))
                {
                    builder.Add(key);
                    builder.Add(Bits.SwapBytes(number3));
                    itemsTable.Set(builder);
                }
                
                Assert.False(itemsTable.DeleteByIndex(fixedSizedIndex, 0L));
                Assert.True(itemsTable.DeleteByIndex(fixedSizedIndex, 2L));
                Assert.True(itemsTable.DeleteByIndex(fixedSizedIndex, 3L));
                Assert.True(itemsTable.DeleteByIndex(fixedSizedIndex, 1L));
            }
        }
        // The FSI cache in Table addresses trees by FixedSizeKeyIndexDef.CachePosition. This test
        // pins the two invariants that make that safe: DefineFixedSizeIndex assigns dense,
        // define-order positions, and GetFixedSizeTree resolves each position back to the tree with
        // the matching name (so a position never aliases the wrong index within a schema).
        [RavenFact(RavenTestCategory.Voron)]
        public void FixedSizeIndexPositionsAreDenseAndResolveByName()
        {
            Slice.From(Allocator, "EtagA", out var a);
            Slice.From(Allocator, "EtagB", out var b);
            Slice.From(Allocator, "EtagC", out var c);

            var defA = new TableSchema.FixedSizeKeyIndexDef { Name = a, IsGlobal = false, StartIndex = 1 };
            var defB = new TableSchema.FixedSizeKeyIndexDef { Name = b, IsGlobal = true, StartIndex = 1 };
            var defC = new TableSchema.FixedSizeKeyIndexDef { Name = c, IsGlobal = false, StartIndex = 1 };

            var schema = new TableSchema()
                .DefineKey(new TableSchema.IndexDef { StartIndex = 0, Count = 1 })
                .DefineFixedSizeIndex(defA)
                .DefineFixedSizeIndex(defB)
                .DefineFixedSizeIndex(defC);

            // dense, in define order
            Assert.Equal(0, defA.CachePosition);
            Assert.Equal(1, defB.CachePosition);
            Assert.Equal(2, defC.CachePosition);
            Assert.Equal(3, schema.FixedSizeIndexes.Count);

            // a def registered in another schema keeps a consistent position when defined in the
            // same order - this is what lets base/compressed schema variants share positions
            var defA2 = new TableSchema.FixedSizeKeyIndexDef { Name = a, IsGlobal = false, StartIndex = 1 };
            var defB2 = new TableSchema.FixedSizeKeyIndexDef { Name = b, IsGlobal = true, StartIndex = 1 };
            new TableSchema()
                .DefineKey(new TableSchema.IndexDef { StartIndex = 0, Count = 1 })
                .DefineFixedSizeIndex(defA2)
                .DefineFixedSizeIndex(defB2);
            Assert.Equal(defA.CachePosition, defA2.CachePosition);
            Assert.Equal(defB.CachePosition, defB2.CachePosition);

            using (var tx = Env.WriteTransaction())
            {
                schema.Create(tx, "Items", 16);
                var table = tx.OpenTable(schema, "Items");

                // each def resolves, by its cache position, to a fixed size tree whose name matches
                foreach (var def in new[] { defA, defB, defC })
                {
                    var tree = table.GetFixedSizeTree(def);
                    Assert.True(SliceComparer.Equals(def.Name, tree.Name),
                        $"position {def.CachePosition} resolved to tree '{tree.Name}' but the def is '{def.Name}'");
                }
                tx.Commit();
            }
        }

    }
}
