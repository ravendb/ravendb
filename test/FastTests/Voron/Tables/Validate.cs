using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Sparrow;
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
                    Name = Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable)
                };

                var actualIndex = new TableSchema.SchemaIndexDef
                {
                    StartIndex = 1,
                    Count = 1,
                    Name = Slice.From(tx.Allocator, "Bad Test Name", ByteStringType.Immutable)
                };

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
                    Name = Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable)
                };

                var actualIndex = new TableSchema.FixedSizeSchemaIndexDef
                {
                    StartIndex = 5,
                    Name = Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable)
                };

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

                expectedSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
                {
                    StartIndex = 2,
                    Name = Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable)
                });

                var actualSchema = new TableSchema();

                actualSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
                {
                    StartIndex = 4,
                    Name = Slice.From(tx.Allocator, "Bad Test Name", ByteStringType.Immutable)
                });


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

                expectedSchema.DefineIndex(new TableSchema.SchemaIndexDef
                {
                    Count = 3,
                    StartIndex = 2,
                    Name = Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable)
                });

                var actualSchema = new TableSchema();

                actualSchema.DefineIndex(new TableSchema.SchemaIndexDef
                {
                    StartIndex = 4,
                    Name = Slice.From(tx.Allocator, "Bad Test Name", ByteStringType.Immutable)
                });


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

                expectedSchema.DefineIndex(new TableSchema.SchemaIndexDef
                {
                    Count = 3,
                    StartIndex = 2,
                    Name = Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable)
                });

                var actualSchema = new TableSchema();

                actualSchema.DefineIndex(new TableSchema.SchemaIndexDef
                {
                    Count = 3,
                    StartIndex = 2,
                    Name = Slice.From(tx.Allocator, "Test Name", ByteStringType.Immutable)
                });

                expectedSchema.Validate(actualSchema);


                actualSchema.DefineIndex(new TableSchema.SchemaIndexDef
                {
                    StartIndex = 4,
                    Name = Slice.From(tx.Allocator, "Bad Test Name", ByteStringType.Immutable)
                });


                Assert.Throws<ArgumentNullException>(delegate { expectedSchema.Validate(null); });
                Assert.Throws<ArgumentException>(delegate { expectedSchema.Validate(actualSchema); });
            }
        }
    }
}
