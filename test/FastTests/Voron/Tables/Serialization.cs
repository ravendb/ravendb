using Sparrow;
using Voron;
using Voron.Data.Tables;
using Xunit;

namespace FastTests.Voron.Tables
{
    public unsafe class Serialization : StorageTest
    {
        [Fact]
        public void CanSerializeNormalIndex()
        {
            using (var tx = Env.WriteTransaction())
            {
                var expectedNormalIndex = new TableSchema.SchemaIndexDef
                {
                    StartIndex = 2,
                    Count = 1,
                    Name = "Test Name",
                    NameAsSlice = Slice.From(StorageEnvironment.LabelsContext, "Test Name", ByteStringType.Immutable)
                };

                byte[] serialized = expectedNormalIndex.Serialize();

                fixed (byte* serializedPtr = serialized)
                {
                    var actualNormalIndex = TableSchema.SchemaIndexDef.ReadFrom(tx.Allocator, serializedPtr, serialized.Length);
                    Assert.Equal(expectedNormalIndex.Serialize(), actualNormalIndex.Serialize());
                }
            }
        }

        [Fact]
        public void CanSerializeFixedIndex()
        {
            using (var tx = Env.WriteTransaction())
            {
                var expectedFixedIndex = new TableSchema.FixedSizeSchemaIndexDef()
                {
                    StartIndex = 2,
                    IsGlobal = true,
                    Name = "Test Name 2",
                    NameAsSlice = Slice.From(StorageEnvironment.LabelsContext, "Test Name 2", ByteStringType.Immutable)
                };

                byte[] serialized = expectedFixedIndex.Serialize();

                fixed (byte* serializedPtr = serialized)
                {
                    var actualFixedIndex = TableSchema.FixedSizeSchemaIndexDef.ReadFrom(tx.Allocator, serializedPtr, serialized.Length);
                    Assert.Equal(expectedFixedIndex.Serialize(), actualFixedIndex.Serialize());
                }
            }
        }

        [Fact]
        public void CanSerializeSchema()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tableSchema = new TableSchema()
                    .DefineIndex("Index 1", new TableSchema.SchemaIndexDef
                    {
                        StartIndex = 2,
                        Count = 1,
                    })
                    .DefineFixedSizeIndex("Index 2", new TableSchema.FixedSizeSchemaIndexDef()
                    {
                        StartIndex = 2,
                        IsGlobal = true
                    })
                    .DefineKey(new TableSchema.SchemaIndexDef
                    {
                        StartIndex = 3,
                        Count = 1,
                    });

                byte[] serialized = tableSchema.SerializeSchema();

                fixed (byte* ptr = serialized)
                {
                    var actualTableSchema = TableSchema.ReadFrom(tx.Allocator, ptr, serialized.Length);
                    Assert.Equal(serialized, actualTableSchema.SerializeSchema());
                }
            }
        }

    }
}
