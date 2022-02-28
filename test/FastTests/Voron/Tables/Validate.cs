using System;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Tables
{
    public class Validate : StorageTest
    {
        public Validate(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ErrorsOnInvalidVariableSizeDef()
        {
            using (var tx = Env.WriteTransaction())
            {
                var expectedIndex = new TableSchema.IndexDef
                {
                    StartIndex = 2,
                    Count = 1,
                    
                };
                Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable, out expectedIndex.Name);

                var actualIndex = new TableSchema.IndexDef
                {
                    StartIndex = 1,
                    Count = 1,
                };
                Slice.From(tx.Allocator, "Bad Test Name", ByteStringType.Immutable, out actualIndex.Name);

                Assert.Throws<ArgumentNullException>(delegate { expectedIndex.EnsureIdentical(null); });
                Assert.Throws<ArgumentException>(delegate { expectedIndex.EnsureIdentical(actualIndex); });
            }
        }

        [Fact]
        public void ErrorsOnInvalidFixedSizeDef()
        {
            using (var tx = Env.WriteTransaction())
            {
                var expectedIndex = new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = 2,
                };
                Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable, out expectedIndex.Name);

                var actualIndex = new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = 5,
                };
                Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable, out actualIndex.Name);

                Assert.Throws<ArgumentNullException>(delegate { expectedIndex.EnsureIdentical(null); });
                Assert.Throws<ArgumentException>(delegate { expectedIndex.EnsureIdentical(actualIndex); });
            }
        }

        [Fact]
        public void ErrorsOnInvalidSchemaWithSingleFixedIndex()
        {
            using (var tx = Env.WriteTransaction())
            {
                var expectedSchema = new TableSchema();

                var def = new TableSchema.FixedSizeKeyIndexDef
                {
                    StartIndex = 2,
                };
                Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable, out def.Name);

                expectedSchema.DefineFixedSizeIndex(def);

                var actualSchema = new TableSchema();

                def = new TableSchema.FixedSizeKeyIndexDef
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

                var def = new TableSchema.IndexDef
                {
                    Count = 3,
                    StartIndex = 2,
                };
                Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable, out def.Name);
                expectedSchema.DefineIndex(def);

                var actualSchema = new TableSchema();

                def = new TableSchema.IndexDef
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

                var def = new TableSchema.IndexDef
                {
                    Count = 3,
                    StartIndex = 2,
                };
                Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable, out def.Name);

                expectedSchema.DefineIndex(def);

                var actualSchema = new TableSchema();

                actualSchema.DefineIndex(def);

                expectedSchema.Validate(actualSchema);

                def = new TableSchema.IndexDef
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
