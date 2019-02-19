using System;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using Xunit;

namespace FastTests.Voron.Tables
{
    public class Validate : StorageTest
    {
        [Fact]
        public void ErrorsOnInvalidVariableSizeDef()
        {
            using (var tx = Env.WriteTransaction())
            {
                var expectedIndex = new TableSchema.SchemaIndexDef
                {
                    StartIndex = 2,
                    Count = 1,
                    
                };
                Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable, out expectedIndex.Name);

                var actualIndex = new TableSchema.SchemaIndexDef
                {
                    StartIndex = 1,
                    Count = 1,
                };
                Slice.From(tx.Allocator, "Bad Test Name", ByteStringType.Immutable, out actualIndex.Name);

                Assert.Throws<ArgumentNullException>(delegate { expectedIndex.Validate(null); });
                Assert.Throws<ArgumentException>(delegate { expectedIndex.Validate(actualIndex); });
            }
        }

        [Fact]
        public void ErrorsOnInvalidFixedSizeDef()
        {
            using (var tx = Env.WriteTransaction())
            {
                var expectedIndex = new TableSchema.FixedSizeSchemaIndexDef
                {
                    StartIndex = 2,
                };
                Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable, out expectedIndex.Name);

                var actualIndex = new TableSchema.FixedSizeSchemaIndexDef
                {
                    StartIndex = 5,
                };
                Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable, out actualIndex.Name);

                Assert.Throws<ArgumentNullException>(delegate { expectedIndex.Validate(null); });
                Assert.Throws<ArgumentException>(delegate { expectedIndex.Validate(actualIndex); });
            }
        }

        [Fact]
        public void ErrorsOnInvalidSchemaWithSingleFixedIndex()
        {
            using (var tx = Env.WriteTransaction())
            {
                var expectedSchema = new TableSchema();

                var def = new TableSchema.FixedSizeSchemaIndexDef
                {
                    StartIndex = 2,
                };
                Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable, out def.Name);

                expectedSchema.DefineFixedSizeIndex(def);

                var actualSchema = new TableSchema();

                def = new TableSchema.FixedSizeSchemaIndexDef
                {
                    StartIndex = 4,
                };
                Slice.From(tx.Allocator, "Bad Test Name", ByteStringType.Immutable, out def.Name);
                actualSchema.DefineFixedSizeIndex(def);


                Assert.Throws<ArgumentNullException>(delegate { expectedSchema.Validate(null); });
                Assert.Throws<ArgumentException>(delegate { expectedSchema.Validate(actualSchema); });
            }
        }

        [Fact]
        public void ErrorsOnInvalidSchemaWithSingleVariableIndex()
        {
            using (var tx = Env.WriteTransaction())
            {
                var expectedSchema = new TableSchema();

                var def = new TableSchema.SchemaIndexDef
                {
                    Count = 3,
                    StartIndex = 2,
                };
                Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable, out def.Name);
                expectedSchema.DefineIndex(def);

                var actualSchema = new TableSchema();

                def = new TableSchema.SchemaIndexDef
                {
                    StartIndex = 4,
                };
                Slice.From(tx.Allocator, "Bad Test Name", ByteStringType.Immutable, out def.Name);

                actualSchema.DefineIndex(def);


                Assert.Throws<ArgumentNullException>(delegate { expectedSchema.Validate(null); });
                Assert.Throws<ArgumentException>(delegate { expectedSchema.Validate(actualSchema); });
            }
        }

        [Fact]
        public void ErrorsOnInvalidSchemaWithMultipleIndexes()
        {
            using (var tx = Env.WriteTransaction())
            {
                var expectedSchema = new TableSchema();

                var def = new TableSchema.SchemaIndexDef
                {
                    Count = 3,
                    StartIndex = 2,
                };
                Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable, out def.Name);

                expectedSchema.DefineIndex(def);

                var actualSchema = new TableSchema();

                actualSchema.DefineIndex(def);

                expectedSchema.Validate(actualSchema);

                def = new TableSchema.SchemaIndexDef
                {
                    StartIndex = 4,
                };
                Slice.From(tx.Allocator, "Bad Test Name", ByteStringType.Immutable, out def.Name);

                actualSchema.DefineIndex(def);


                Assert.Throws<ArgumentNullException>(delegate { expectedSchema.Validate(null); });
                Assert.Throws<ArgumentException>(delegate { expectedSchema.Validate(actualSchema); });
            }
        }
    }
}
