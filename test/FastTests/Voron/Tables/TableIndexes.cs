using Sparrow.Binary;
using Voron;
using Voron.Data.Tables;
using Voron.Exceptions;
using Xunit;

namespace FastTests.Voron.Tables
{
    public class TableIndexes : StorageTest
    {
        [Fact]
        public void Insert_same_value_to_fixed_sized_index_throws()
        {
            using (var tx = Env.WriteTransaction())
            {
                Slice.From(tx.Allocator, "EtagIndexName", out var etagIndexName);
                var fixedSizedIndex = new TableSchema.FixedSizeSchemaIndexDef
                {
                    Name = etagIndexName,
                    IsGlobal = true,
                    StartIndex = 1,
                };

                var tableSchema = new TableSchema()
                    .DefineFixedSizeIndex(fixedSizedIndex)
                    .DefineKey(new TableSchema.SchemaIndexDef
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

        [Fact]
        public void Update_same_value_to_fixed_sized_index_throws()
        {
            using (var tx = Env.WriteTransaction())
            {
                Slice.From(tx.Allocator, "EtagIndexName", out var etagIndexName);
                var fixedSizedIndex = new TableSchema.FixedSizeSchemaIndexDef
                {
                    Name = etagIndexName,
                    IsGlobal = true,
                    StartIndex = 1,
                };

                var tableSchema = new TableSchema()
                    .DefineFixedSizeIndex(fixedSizedIndex)
                    .DefineKey(new TableSchema.SchemaIndexDef
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

        [Fact]
        public void Insert_same_value_to_index_deosnt_throw()
        {
            using (var tx = Env.WriteTransaction())
            {
                Slice.From(tx.Allocator, "EtagIndexName", out var etagIndexName);
                var index = new TableSchema.SchemaIndexDef
                {
                    Name = etagIndexName,
                    StartIndex = 0,
                    Count = 1
                };

                var tableSchema = new TableSchema()
                    .DefineIndex(index)
                    .DefineKey(new TableSchema.SchemaIndexDef
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

        [Fact]
        public void Update_same_value_to_index_doesnt_throw()
        {
            using (var tx = Env.WriteTransaction())
            {
                Slice.From(tx.Allocator, "EtagIndexName", out var etagIndexName);
                var index = new TableSchema.SchemaIndexDef
                {
                    Name = etagIndexName,
                    StartIndex = 0,
                    Count = 1
                };

                var tableSchema = new TableSchema()
                    .DefineIndex(index)
                    .DefineKey(new TableSchema.SchemaIndexDef
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
    }
}
