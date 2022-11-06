using System;
using System.Linq;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using Xunit;
using Xunit.Abstractions;
using static FastTests.Voron.Tables.RavenDB_17760;

namespace FastTests.Voron.Tables
{
    public unsafe class Serialization : StorageTest
    {
        public Serialization(ITestOutputHelper output) : base(output)
        {
        }

        private static void SchemaIndexDefEqual(TableSchema.AbstractTreeIndexDef expectedIndex,
            TableSchema.AbstractTreeIndexDef actualIndex)
        {
            if (expectedIndex == null)
            {
                Assert.Equal(null, actualIndex);
            }
            else
            {
                Assert.Equal(expectedIndex.IsGlobal, actualIndex.IsGlobal);
                Assert.True(SliceComparer.Equals(expectedIndex.Name, actualIndex.Name));
                Assert.Equal(expectedIndex.Type, actualIndex.Type);

                switch (expectedIndex.Type)
                {
                    case TableSchema.TreeIndexType.Default:
                    {
                        Assert.IsType<TableSchema.IndexDef>(expectedIndex);
                        Assert.IsType<TableSchema.IndexDef>(actualIndex);

                        var expected = (TableSchema.IndexDef)expectedIndex;
                        var actual = (TableSchema.IndexDef)actualIndex;

                        Assert.Equal(expected.StartIndex, actual.StartIndex);
                        Assert.Equal(expected.Count, actual.Count);

                        break;
                    }
                    case TableSchema.TreeIndexType.DynamicKeyValues:
                    {
                        Assert.IsType<TableSchema.DynamicKeyIndexDef>(expectedIndex);
                        Assert.IsType<TableSchema.DynamicKeyIndexDef>(actualIndex);

                        var expected = (TableSchema.DynamicKeyIndexDef)expectedIndex;
                        var actual = (TableSchema.DynamicKeyIndexDef)actualIndex;

                        Assert.Equal(expected.GenerateKey.Method.Name, actual.GenerateKey.Method.Name);
                        Assert.Equal(expected.GenerateKey.Method.DeclaringType, actual.GenerateKey.Method.DeclaringType);

                        Assert.Equal(expected.OnEntryChanged?.Method.Name, actual.OnEntryChanged?.Method.Name);
                        Assert.Equal(expected.OnEntryChanged?.Method.DeclaringType, actual.OnEntryChanged?.Method.DeclaringType);

                        break;
                    }
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }

        private static void FixedSchemaIndexDefEqual(TableSchema.FixedSizeKeyIndexDef expectedIndex,
            TableSchema.FixedSizeKeyIndexDef actualIndex)
        {
            if (expectedIndex == null)
            {
                Assert.Equal(null, actualIndex);
            }
            else
            {
                Assert.Equal(expectedIndex.IsGlobal, actualIndex.IsGlobal);
                Assert.True(SliceComparer.Equals(expectedIndex.Name, actualIndex.Name));
                Assert.Equal(expectedIndex.StartIndex, actualIndex.StartIndex);
            }
        }

        private static void SchemaDefEqual(TableSchema expected, TableSchema actual)
        {
            // Same primary keys
            SchemaIndexDefEqual(expected.Key, actual.Key);
            // Same keys for variable size indexes
            Assert.Equal(expected.Indexes.Keys.Count, actual.Indexes.Keys.Count);
            Assert.Equal(expected.Indexes.Keys.Count, expected.Indexes.Keys.Intersect(actual.Indexes.Keys, SliceComparer.Instance).Count());
            // Same keys for fixed size indexes
            Assert.Equal(expected.FixedSizeIndexes.Keys.Count, actual.FixedSizeIndexes.Keys.Count);
            Assert.Equal(expected.FixedSizeIndexes.Keys.Count, expected.FixedSizeIndexes.Keys.Intersect(actual.FixedSizeIndexes.Keys, SliceComparer.Instance).Count());
            // Same keys for dynamic-key indexes
            Assert.Equal(expected.DynamicKeyIndexes.Keys.Count, actual.DynamicKeyIndexes.Keys.Count);
            Assert.Equal(expected.DynamicKeyIndexes.Keys.Count, expected.DynamicKeyIndexes.Keys.Intersect(actual.DynamicKeyIndexes.Keys, SliceComparer.Instance).Count());

            // Same indexes
            foreach (var entry in expected.Indexes)
            {
                var other = actual.Indexes[entry.Key];
                SchemaIndexDefEqual(entry.Value, other);
            }

            foreach (var entry in expected.FixedSizeIndexes)
            {
                var other = actual.FixedSizeIndexes[entry.Key];
                FixedSchemaIndexDefEqual(entry.Value, other);
            }

            foreach (var entry in expected.DynamicKeyIndexes)
            {
                var other = actual.DynamicKeyIndexes[entry.Key];
                SchemaIndexDefEqual(entry.Value, other);
            }
        }

        [Fact]
        public void CanSerializeNormalIndex()
        {
            using (var tx = Env.WriteTransaction())
            {
                var expectedIndex = new TableSchema.IndexDef
                {
                    StartIndex = 2,
                    Count = 1,
                };
                Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable, out expectedIndex.Name);

                byte[] serialized = expectedIndex.Serialize();

                fixed (byte* serializedPtr = serialized)
                {
                    var reader = new TableValueReader(serializedPtr, serialized.Length);
                    var actualIndex = TableSchema.IndexDef.ReadFrom(tx.Allocator, ref reader);
                    Assert.Equal(serialized, actualIndex.Serialize());
                    SchemaIndexDefEqual(expectedIndex, actualIndex);
                    expectedIndex.EnsureIdentical(actualIndex);
                }
            }
        }

        [Fact]
        public void CanSerializeFixedIndex()
        {
            using (var tx = Env.WriteTransaction())
            {
                var expectedIndex = new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = 2,
                    IsGlobal = true,
                };
                Slice.From(tx.Allocator, "Test Name 2", ByteStringType.Immutable, out expectedIndex.Name);

                byte[] serialized = expectedIndex.Serialize();

                fixed (byte* serializedPtr = serialized)
                {
                    var actualIndex = TableSchema.FixedSizeKeyIndexDef.ReadFrom(tx.Allocator, serializedPtr, serialized.Length);
                    Assert.Equal(serialized, actualIndex.Serialize());
                    FixedSchemaIndexDefEqual(expectedIndex, actualIndex);
                    expectedIndex.EnsureIdentical(actualIndex);
                }
            }
        }

        [Fact]
        public void CanSerializeSchema()
        {
            using (var tx = Env.WriteTransaction())
            {
                var def1 = new TableSchema.IndexDef
                {
                    StartIndex = 2,
                    Count = 1,
                };
                Slice.From(tx.Allocator, "Test Name 2", ByteStringType.Immutable, out def1.Name);

                var def2 = new TableSchema.FixedSizeKeyIndexDef()
                {
                    StartIndex = 2,
                    IsGlobal = true,
                };
                Slice.From(tx.Allocator, "Test Name 1", ByteStringType.Immutable, out def2.Name);

                var tableSchema = new TableSchema()
                    .DefineIndex(def1)
                    .DefineFixedSizeIndex(def2)
                    .DefineKey(new TableSchema.IndexDef
                    {
                        StartIndex = 3,
                        Count = 1,
                    });

                byte[] serialized = tableSchema.SerializeSchema();

                fixed (byte* ptr = serialized)
                {
                    var actualTableSchema = TableSchema.ReadFrom(tx.Allocator, ptr, serialized.Length);
                    // This checks that reserializing is the same
                    Assert.Equal(serialized, actualTableSchema.SerializeSchema());
                    // This checks that what was deserialized is correct
                    SchemaDefEqual(tableSchema, actualTableSchema);
                    tableSchema.Validate(actualTableSchema);
                }
            }
        }
        
        [Fact]
        public void CanSerializeMultiIndexSchema()
        {
            using (var tx = Env.WriteTransaction())
            {
                var def1 = new TableSchema.IndexDef
                {
                    StartIndex = 2,
                    Count = 1,
                };
                Slice.From(tx.Allocator, "Test Name 1", ByteStringType.Immutable, out def1.Name);

                var def2 = new TableSchema.IndexDef
                {
                    StartIndex = 1,
                    Count = 1,
                };
                Slice.From(tx.Allocator, "Test Name 2", ByteStringType.Immutable, out def2.Name);

                var def3 = new TableSchema.FixedSizeKeyIndexDef()
                {
                    StartIndex = 2,
                    IsGlobal = true,
                };
                Slice.From(tx.Allocator, "Test Name 3", ByteStringType.Immutable, out def3.Name);

                var tableSchema = new TableSchema()
                    .DefineIndex(def1)
                    .DefineIndex(def2)
                    .DefineFixedSizeIndex(def3)
                    .DefineKey(new TableSchema.IndexDef
                    {
                        StartIndex = 3,
                        Count = 1,
                    });

                byte[] serialized = tableSchema.SerializeSchema();

                fixed (byte* ptr = serialized)
                {
                    var actualTableSchema = TableSchema.ReadFrom(tx.Allocator, ptr, serialized.Length);
                    // This checks that reserializing is the same
                    Assert.Equal(serialized, actualTableSchema.SerializeSchema());
                    // This checks that what was deserialized is correct
                    SchemaDefEqual(tableSchema, actualTableSchema);
                    tableSchema.Validate(actualTableSchema);
                }
            }
        }

        [Fact]
        public void CanSerializeDynamicIndex()
        {
            using (var tx = Env.WriteTransaction())
            {
                var expectedIndex = new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = IndexKeyGenerator
                };
                Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable, out expectedIndex.Name);

                byte[] serialized = expectedIndex.Serialize();

                fixed (byte* serializedPtr = serialized)
                {
                    var reader = new TableValueReader(serializedPtr, serialized.Length);
                    var actualIndex = TableSchema.DynamicKeyIndexDef.ReadFrom(tx.Allocator, ref reader);
                    Assert.Equal(serialized, actualIndex.Serialize());
                    SchemaIndexDefEqual(expectedIndex, actualIndex);
                    expectedIndex.EnsureIdentical(actualIndex);
                }
            }
        }

        [Fact]
        public void CanSerializeSchemaWithDynamicIndex()
        {
            using (var tx = Env.WriteTransaction())
            {
                var def1 = new TableSchema.IndexDef
                {
                    StartIndex = 5,
                    Count = 2,
                };
                Slice.From(tx.Allocator, "Test Name 1", ByteStringType.Immutable, out def1.Name);

                var def2 = new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = 2,
                    IsGlobal = true
                };
                Slice.From(tx.Allocator, "Test Name 2", ByteStringType.Immutable, out def2.Name);

                var def3 = new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = IndexKeyGenerator
                };
                Slice.From(tx.Allocator, "Test Name 3", ByteStringType.Immutable, out def3.Name);

                var tableSchema = new TableSchema()
                    .DefineIndex(def1)
                    .DefineFixedSizeIndex(def2)
                    .DefineIndex(def3)
                    .DefineKey(new TableSchema.IndexDef
                    {
                        StartIndex = 3,
                        Count = 1
                    });

                byte[] serialized = tableSchema.SerializeSchema();

                fixed (byte* ptr = serialized)
                {
                    var actualTableSchema = TableSchema.ReadFrom(tx.Allocator, ptr, serialized.Length);
                    // This checks that reserializing is the same
                    Assert.Equal(serialized, actualTableSchema.SerializeSchema());
                    // This checks that what was deserialized is correct
                    SchemaDefEqual(tableSchema, actualTableSchema);
                    tableSchema.Validate(actualTableSchema);
                }
            }
        }

        [Fact]
        public void CanSerializeDynamicIndex2()
        {
            using (var tx = Env.WriteTransaction())
            {
                var expectedIndex = new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = IndexKeyGenerator,
                    OnEntryChanged = UpdateStats
                };
                Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable, out expectedIndex.Name);

                byte[] serialized = expectedIndex.Serialize();

                fixed (byte* serializedPtr = serialized)
                {
                    var reader = new TableValueReader(serializedPtr, serialized.Length);
                    var actualIndex = TableSchema.DynamicKeyIndexDef.ReadFrom(tx.Allocator, ref reader);
                    Assert.Equal(serialized, actualIndex.Serialize());
                    SchemaIndexDefEqual(expectedIndex, actualIndex);
                    expectedIndex.EnsureIdentical(actualIndex);
                }
            }
        }

        [Fact]
        public void CanSerializeSchemaWithDynamicIndex2()
        {
            using (var tx = Env.WriteTransaction())
            {
                var def1 = new TableSchema.IndexDef
                {
                    StartIndex = 5,
                    Count = 2,
                };
                Slice.From(tx.Allocator, "Test Name 1", ByteStringType.Immutable, out def1.Name);

                var def2 = new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = 2,
                    IsGlobal = true
                };
                Slice.From(tx.Allocator, "Test Name 2", ByteStringType.Immutable, out def2.Name);

                var def3 = new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = IndexKeyGenerator,
                    OnEntryChanged = UpdateStats
                };
                Slice.From(tx.Allocator, "Test Name 3", ByteStringType.Immutable, out def3.Name);
                
                var tableSchema = new TableSchema()
                    .DefineIndex(def1)
                    .DefineFixedSizeIndex(def2)
                    .DefineIndex(def3)
                    .DefineKey(new TableSchema.IndexDef
                    {
                        StartIndex = 3,
                        Count = 1
                    });

                byte[] serialized = tableSchema.SerializeSchema();

                fixed (byte* ptr = serialized)
                {
                    var actualTableSchema = TableSchema.ReadFrom(tx.Allocator, ptr, serialized.Length);
                    // This checks that reserializing is the same
                    Assert.Equal(serialized, actualTableSchema.SerializeSchema());
                    // This checks that what was deserialized is correct
                    SchemaDefEqual(tableSchema, actualTableSchema);
                    tableSchema.Validate(actualTableSchema);
                }
            }
        }
    }
}

