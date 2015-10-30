// -----------------------------------------------------------------------
//  <copyright file="AddingStructValues.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using System.IO;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Trees
{
    public class RavenDB_3114_WorkingWithStructs : StorageTest
    {
        class Foo
        {
             
        }

        public enum SchemaFields
        {
            Message,
            Count,
            DateTime,
            Does_Not_Exist,
            Additional_Info
        }


        [PrefixesFact]
        public void ShouldNotAllowToCreateSchemaWithAnyTypeOfFields()
        {
            Assert.DoesNotThrow(() => new StructureSchema<MappedResults>());

            Assert.Throws<ArgumentException>(() => new StructureSchema<string>());
            Assert.Throws<ArgumentException>(() => new StructureSchema<int>());
            Assert.Throws<ArgumentException>(() => new StructureSchema<ushort>());
            Assert.Throws<ArgumentException>(() => new StructureSchema<byte>());
            var ae = Assert.Throws<ArgumentException>(() => new StructureSchema<Foo>());

            Assert.Equal("IStructure schema can only have fields of enum type.", ae.Message);
        }


        [PrefixesFact]
        public void ShouldNotAllowToDefineFixedSizeFieldAfterVariableSizeField()
        {
            var ae = Assert.Throws<ArgumentException>(() => new StructureSchema<SchemaFields>().Add<string>(SchemaFields.Message).Add<int>(SchemaFields.Count));

            Assert.Equal("Cannot define a fixed size field after variable size fields", ae.Message);
        }

        [PrefixesFact]
        public void ShouldThrowOnUnsupportedType()
        {
            var notSupportedException = Assert.Throws<NotSupportedException>(() => new StructureSchema<SchemaFields>().Add<DateTime>(SchemaFields.DateTime));
            Assert.Equal("Not supported structure field type: System.DateTime", notSupportedException.Message);

            notSupportedException = Assert.Throws<NotSupportedException>(() => new StructureSchema<SchemaFields>().Add<Foo>(SchemaFields.Count));
            Assert.True(notSupportedException.Message.StartsWith("Not supported structure field type:"));
        }

        [PrefixesFact]
        public void ShouldThrowWhenSettingUndefinedField()
        {
            var schema = new StructureSchema<SchemaFields>()
                .Add<int>(SchemaFields.Count)
                .Add<string>(SchemaFields.Message);

            var structure = new Structure<SchemaFields>(schema);

            Assert.DoesNotThrow(() => structure.Set(SchemaFields.Count, 1));
            Assert.DoesNotThrow(() => structure.Set(SchemaFields.Message, "hello"));

            var ae = Assert.Throws<ArgumentException>(() => structure.Set(SchemaFields.Does_Not_Exist, 1));
            Assert.Equal("No such field in schema defined. Field name: Does_Not_Exist", ae.Message);

            ae = Assert.Throws<ArgumentException>(() => structure.Set(SchemaFields.Does_Not_Exist, "hello"));
            Assert.Equal("No such field in schema defined. Field name: Does_Not_Exist", ae.Message);

            ae = Assert.Throws<ArgumentException>(() => structure.Increment(SchemaFields.Does_Not_Exist, 1));
            Assert.Equal("No such field in schema defined. Field name: Does_Not_Exist", ae.Message);
        }

        [PrefixesFact]
        public void ShouldThrowWhenSettingDifferentValueTypeThanDefinedInSchema()
        {
            var schema = new StructureSchema<SchemaFields>()
                .Add<int>(SchemaFields.Count);

            var structure = new Structure<SchemaFields>(schema);

            var invalidData = Assert.Throws<InvalidDataException>(() => structure.Set(SchemaFields.Count, (long) 1));
            Assert.Equal("Attempt to set a field value which type is different than defined in the structure schema. Expected: System.Int32, got: System.Int64", invalidData.Message);
        }

        [PrefixesFact]
        public void ShouldThrowOnAttemptToPartialWriteOfVariableFields()
        {
            var schema = new StructureSchema<SchemaFields>()
                .Add<string>(SchemaFields.Message)
                .Add<string>(SchemaFields.Additional_Info);

            var structure = new Structure<SchemaFields>(schema);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "stats");

                structure.Set(SchemaFields.Message, "hello");

                var invalidOperationException = Assert.Throws<InvalidOperationException>(() => tree.WriteStruct("items/1", structure));

                Assert.Equal("Your structure has variable size fields. You have to set all of them to properly write a structure and avoid overlapping fields. Missing fields: Additional_Info", invalidOperationException.Message);
            }
        }

        [PrefixesFact]
        public void ShouldNotAllowToDefineSchemaWithDuplicatedFields()
        {
            var ae = Assert.Throws<ArgumentException>(() => new StructureSchema<SchemaFields>()
                .Add<string>(SchemaFields.Message)
                .Add<string>(SchemaFields.Message));

            Assert.Equal("Field 'Message' is already defined", ae.Message);

            ae = Assert.Throws<ArgumentException>(() => new StructureSchema<SchemaFields>()
                .Add<int>(SchemaFields.Message)
                .Add<string>(SchemaFields.Message));

            Assert.Equal("Field 'Message' is already defined", ae.Message);

            ae = Assert.Throws<ArgumentException>(() => new StructureSchema<SchemaFields>()
                .Add<byte[]>(SchemaFields.Message)
                .Add<long>(SchemaFields.Message));

            Assert.Equal("Field 'Message' is already defined", ae.Message);
        }

        internal enum IndexingStatsFields
        {
            Attempts,
            Errors,
            Successes,
            IsValid,
            IndexedAt,
            Message
        }

        [PrefixesFact]
        public void CanReadAndWriteStructsFromTrees()
        {
            var indexedAt = new DateTime(2015, 1, 20);

            var schema = new StructureSchema<IndexingStatsFields>()
                .Add<int>(IndexingStatsFields.Attempts)
                .Add<int>(IndexingStatsFields.Errors)
                .Add<int>(IndexingStatsFields.Successes)
                .Add<byte>(IndexingStatsFields.IsValid)
                .Add<long>(IndexingStatsFields.IndexedAt);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "stats");

                var stats = new Structure<IndexingStatsFields>(schema);

                stats.Set(IndexingStatsFields.Attempts, 5);
                stats.Set(IndexingStatsFields.Errors, -1);
                stats.Set(IndexingStatsFields.Successes, 4);
                stats.Set(IndexingStatsFields.IsValid, (byte) 1);
                stats.Set(IndexingStatsFields.IndexedAt, indexedAt.ToBinary());

                tree.WriteStruct("stats/1", stats);

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var tree = tx.ReadTree("stats");

                var stats = tree.ReadStruct("stats/1", schema).Reader;

                Assert.Equal(5, stats.ReadInt(IndexingStatsFields.Attempts));
                Assert.Equal(-1, stats.ReadInt(IndexingStatsFields.Errors));
                Assert.Equal(4, stats.ReadInt(IndexingStatsFields.Successes));
                Assert.Equal(1, stats.ReadByte(IndexingStatsFields.IsValid));
                Assert.Equal(indexedAt, DateTime.FromBinary(stats.ReadLong(IndexingStatsFields.IndexedAt)));
            }
        }

        [PrefixesFact]
        public void CanDeleteStructsFromTrees()
        {
            var schema = new StructureSchema<IndexingStatsFields>()
                .Add<int>(IndexingStatsFields.Attempts)
                .Add<int>(IndexingStatsFields.Errors)
                .Add<int>(IndexingStatsFields.Successes);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "stats");

                var stats = new Structure<IndexingStatsFields>(schema);

                stats.Set(IndexingStatsFields.Attempts, 5);
                stats.Set(IndexingStatsFields.Errors, -1);
                stats.Set(IndexingStatsFields.Successes, 4);

                tree.WriteStruct("stats/1", stats);

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = tx.ReadTree("stats");

                tree.Delete("stats/1");

                var stats = tree.ReadStruct("stats/1", schema);

                Assert.Null(stats);
            }
        }


        [PrefixesFact]
        public void CanWriteStructsByUsingWriteBatchAndReadFromSnapshot()
        {
            var statsSchema = new StructureSchema<IndexingStatsFields>()
                .Add<int>(IndexingStatsFields.Attempts)
                .Add<int>(IndexingStatsFields.Errors)
                .Add<int>(IndexingStatsFields.Successes)
                .Add<string>(IndexingStatsFields.Message);

            var operationSchema = new StructureSchema<IndexingStatsFields>()
                .Add<int>(IndexingStatsFields.Attempts)
                .Add<int>(IndexingStatsFields.Successes)
                .Add<string>(IndexingStatsFields.Message);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.CreateTree(tx, "stats");
                Env.CreateTree(tx, "operations");

                tx.Commit();
            }
            var batch = new WriteBatch();

            batch.AddStruct("stats/1",
                new Structure<IndexingStatsFields>(statsSchema)
                .Set(IndexingStatsFields.Attempts, 5)
                .Set(IndexingStatsFields.Errors, -1)
                .Set(IndexingStatsFields.Successes, 4)
                .Set(IndexingStatsFields.Message, "hello world"),
                "stats");

            batch.AddStruct("operations/1",
                new Structure<IndexingStatsFields>(operationSchema)
                .Set(IndexingStatsFields.Attempts, 10)
                .Set(IndexingStatsFields.Successes, 10)
                .Set(IndexingStatsFields.Message, "hello world"),
                "operations");

            using (var snapshot = Env.CreateSnapshot())
            {
                var stats = snapshot.ReadStruct("stats", "stats/1", statsSchema, batch).Reader;

                Assert.Equal(5, stats.ReadInt(IndexingStatsFields.Attempts));
                Assert.Equal(-1, stats.ReadInt(IndexingStatsFields.Errors));
                Assert.Equal(4, stats.ReadInt(IndexingStatsFields.Successes));
                Assert.Equal("hello world", stats.ReadString(IndexingStatsFields.Message));
            }

            Env.Writer.Write(batch);

            using (var snapshot = Env.CreateSnapshot())
            {
                var operation = snapshot.ReadStruct("operations", "operations/1", operationSchema).Reader;

                Assert.Equal(10, operation.ReadInt(IndexingStatsFields.Attempts));
                Assert.Equal(10, operation.ReadInt(IndexingStatsFields.Successes));
                Assert.Equal("hello world", operation.ReadString(IndexingStatsFields.Message));
            }

            batch.Delete("stats/1", "stats");

            using (var snapshot = Env.CreateSnapshot())
            {
                var stats = snapshot.ReadStruct("stats", "stats/1", statsSchema, batch);

                Assert.Null(stats);
            }
        }

        [PrefixesFact]
        public void CanReadStructsFromTreeIterator()
        {
            var statsSchema = new StructureSchema<IndexingStatsFields>()
                .Add<int>(IndexingStatsFields.Attempts)
                .Add<string>(IndexingStatsFields.Message);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "stats");

                tree.WriteStruct("items/1", new Structure<IndexingStatsFields>(statsSchema)
                    .Set(IndexingStatsFields.Attempts, 1)
                    .Set(IndexingStatsFields.Message, "1"));

                tree.WriteStruct("items/2", new Structure<IndexingStatsFields>(statsSchema)
                    .Set(IndexingStatsFields.Attempts, 2)
                    .Set(IndexingStatsFields.Message, "2"));


                tree.WriteStruct("items/3", new Structure<IndexingStatsFields>(statsSchema)
                    .Set(IndexingStatsFields.Attempts, 3)
                    .Set(IndexingStatsFields.Message, "3"));

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var iterator = tx.ReadTree("stats").Iterate();

                iterator.Seek(Slice.BeforeAllKeys);

                var count = 0;

                do
                {
                    var stats = iterator.ReadStructForCurrent(statsSchema);

                    count++;

                    Assert.Equal(count, stats.ReadInt(IndexingStatsFields.Attempts));
                    Assert.Equal(count.ToString(CultureInfo.InvariantCulture), stats.ReadString(IndexingStatsFields.Message));

                } while (iterator.MoveNext());

                Assert.Equal(3, count);
            }
        }

        public enum MappedResults
        {
            View,
            ReduceKey,
            Bucket,
            DocId,
            Etag,
            TimestampBinary
        }

        [PrefixesFact]
        public void BasicStructureWithStringTest()
        {
            var schema = new StructureSchema<Enum>()
                .Add<int>(MappedResults.View)
                .Add<string>(MappedResults.ReduceKey);


            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "stats");

                tree.WriteStruct("items/1", 
                    new Structure<Enum>(schema)
                        .Set(MappedResults.View, 3)
                        .Set(MappedResults.ReduceKey, "reduce_key"));

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var readTree = tx.ReadTree("stats");

                var mappedResults = readTree.ReadStruct("items/1", schema).Reader;

                Assert.Equal(3, mappedResults.ReadInt(MappedResults.View));
                Assert.Equal("reduce_key", mappedResults.ReadString(MappedResults.ReduceKey));
            }
        }

        public enum HelloWorld
        {
            Hello,
            World
        }

        [PrefixesFact]
        public void MultipleStringFieldsShouldBeWrittenInProperOrder()
        {
            var schema = new StructureSchema<HelloWorld>()
                .Add<string>(HelloWorld.Hello)
                .Add<string>(HelloWorld.World);

            var structure = new Structure<HelloWorld>(schema);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "stats");

                structure.Set(HelloWorld.World, "W0rld!"); // set 2nd value first
                structure.Set(HelloWorld.Hello, "Hell0"); // set 1st value in second operation

                tree.WriteStruct("items/1", structure);

                var structReader = tree.ReadStruct("items/1", schema).Reader;

                Assert.Equal("Hell0", structReader.ReadString(HelloWorld.Hello));
                Assert.Equal("W0rld!", structReader.ReadString(HelloWorld.World));
            }
        }

        [PrefixesFact]
        public void ComplexStructureTest()
        {
            var now = DateTime.Now;
            var etag = new byte[16]
            {
                1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6
            };

            var schema = new StructureSchema<MappedResults>()
                .Add<int>(MappedResults.View)
                .Add<long>(MappedResults.Bucket)
                .Add<long>(MappedResults.TimestampBinary)
                .Add<string>(MappedResults.ReduceKey)
                .Add<string>(MappedResults.DocId)
                .Add<byte[]>(MappedResults.Etag);


            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "stats");

                tree.WriteStruct("items/1",
                    new Structure<MappedResults>(schema)
                        .Set(MappedResults.View, 3)
                        .Set(MappedResults.Bucket, 999L)
                        .Set(MappedResults.TimestampBinary, now.ToBinary())
                        .Set(MappedResults.ReduceKey, "reduce_key")
                        .Set(MappedResults.DocId, "orders/1")
                        .Set(MappedResults.Etag, etag));

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                var readTree = tx.ReadTree("stats");

                var mappedResults = readTree.ReadStruct("items/1", schema).Reader;

                Assert.Equal(3, mappedResults.ReadInt(MappedResults.View));
                Assert.Equal("reduce_key", mappedResults.ReadString(MappedResults.ReduceKey));
                Assert.Equal(999, mappedResults.ReadLong(MappedResults.Bucket));
                Assert.Equal("orders/1", mappedResults.ReadString(MappedResults.DocId));
                Assert.Equal(etag, mappedResults.ReadBytes(MappedResults.Etag));
                Assert.Equal(now, DateTime.FromBinary(mappedResults.ReadLong(MappedResults.TimestampBinary)));
            }
        }

        internal enum PrimitiveFields 
        {
            @sbyte,
            @byte,
            @short,
            @ushort,
            @int,
            @uint,
            @long,
            @ulong,
            @char,
            @float,
            @double,
            @decimal,
            @bool
        }

        [PrefixesFact]
        public void CanWriteAndReadAllPrimitiveTypesAndDecimals()
        {
            var schema = new StructureSchema<PrimitiveFields>()
                .Add<sbyte>(PrimitiveFields.@sbyte)
                .Add<byte>(PrimitiveFields.@byte)
                .Add<short>(PrimitiveFields.@short)
                .Add<ushort>(PrimitiveFields.@ushort)
                .Add<int>(PrimitiveFields.@int)
                .Add<uint>(PrimitiveFields.@uint)
                .Add<long>(PrimitiveFields.@long)
                .Add<ulong>(PrimitiveFields.@ulong)
                .Add<char>(PrimitiveFields.@char)
                .Add<float>(PrimitiveFields.@float)
                .Add<double>(PrimitiveFields.@double)
                .Add<decimal>(PrimitiveFields.@decimal)
                .Add<bool>(PrimitiveFields.@bool);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var tree = Env.CreateTree(tx, "primitives");

                tree.WriteStruct("primitives/1",
                    new Structure<PrimitiveFields>(schema)
                        .Set(PrimitiveFields.@sbyte, sbyte.MaxValue)
                        .Set(PrimitiveFields.@byte, byte.MaxValue)
                        .Set(PrimitiveFields.@short, short.MaxValue)
                        .Set(PrimitiveFields.@ushort, ushort.MaxValue)
                        .Set(PrimitiveFields.@int, int.MaxValue)
                        .Set(PrimitiveFields.@uint, uint.MaxValue)
                        .Set(PrimitiveFields.@long, long.MaxValue)
                        .Set(PrimitiveFields.@ulong, ulong.MaxValue)
                        .Set(PrimitiveFields.@char, char.MaxValue)
                        .Set(PrimitiveFields.@float, float.MaxValue)
                        .Set(PrimitiveFields.@double, double.MaxValue)
                        .Set(PrimitiveFields.@decimal, decimal.MaxValue)
                        .Set(PrimitiveFields.@bool, false));

                var primitives = tree.ReadStruct("primitives/1", schema).Reader;

                Assert.Equal(sbyte.MaxValue, primitives.ReadSByte(PrimitiveFields.@sbyte));
                Assert.Equal(byte.MaxValue, primitives.ReadByte(PrimitiveFields.@byte));
                Assert.Equal(short.MaxValue, primitives.ReadShort(PrimitiveFields.@short));
                Assert.Equal(ushort.MaxValue, primitives.ReadUShort(PrimitiveFields.@ushort));
                Assert.Equal(int.MaxValue, primitives.ReadInt(PrimitiveFields.@int));
                Assert.Equal(uint.MaxValue, primitives.ReadUInt(PrimitiveFields.@uint));
                Assert.Equal(long.MaxValue, primitives.ReadLong(PrimitiveFields.@long));
                Assert.Equal(ulong.MaxValue, primitives.ReadULong(PrimitiveFields.@ulong));
                Assert.Equal(char.MaxValue, primitives.ReadChar(PrimitiveFields.@char));
                Assert.Equal(float.MaxValue, primitives.ReadFloat(PrimitiveFields.@float));
                Assert.Equal(double.MaxValue, primitives.ReadDouble(PrimitiveFields.@double));
                Assert.Equal(decimal.MaxValue, primitives.ReadDecimal(PrimitiveFields.@decimal));
                Assert.Equal(false, primitives.ReadBool(PrimitiveFields.@bool));


                tree.WriteStruct("primitives/1",
                    new Structure<PrimitiveFields>(schema)
                        .Increment(PrimitiveFields.@sbyte, -1 * sbyte.MaxValue)
                        .Increment(PrimitiveFields.@short, -1 * short.MaxValue)
                        .Increment(PrimitiveFields.@int, -1 * int.MaxValue)
                        .Increment(PrimitiveFields.@long, -1 * long.MaxValue));

                primitives = tree.ReadStruct("primitives/1", schema).Reader;

                Assert.Equal(0, primitives.ReadSByte(PrimitiveFields.@sbyte));
                Assert.Equal(0, primitives.ReadShort(PrimitiveFields.@short));
                Assert.Equal(0, primitives.ReadInt(PrimitiveFields.@int));
                Assert.Equal(0, primitives.ReadLong(PrimitiveFields.@long));
                

                tree.WriteStruct("primitives/2",
                    new Structure<PrimitiveFields>(schema)
                        .Set(PrimitiveFields.@byte, (byte) 0)
                        .Set(PrimitiveFields.@ushort, (ushort) 0)
                        .Set(PrimitiveFields.@uint, (uint) 0)
                        .Set(PrimitiveFields.@ulong, (ulong) 0)
                        .Set(PrimitiveFields.@float, 0f)
                        .Set(PrimitiveFields.@double, 0d)
                        .Set(PrimitiveFields.@decimal, (decimal) 0)
                        .Set(PrimitiveFields.@char, 'a'));

                tree.WriteStruct("primitives/2",
                    new Structure<PrimitiveFields>(schema)
                        .Increment(PrimitiveFields.@byte, 1)
                        .Increment(PrimitiveFields.@ushort, 2)
                        .Increment(PrimitiveFields.@uint, 3)
                        .Increment(PrimitiveFields.@ulong, 4)
                        .Increment(PrimitiveFields.@float, -1)
                        .Increment(PrimitiveFields.@double, -2)
                        .Increment(PrimitiveFields.@decimal, -3)
                        .Increment(PrimitiveFields.@char, 1));

                var primitives2 = tree.ReadStruct("primitives/2", schema).Reader;

                Assert.Equal(1, primitives2.ReadByte(PrimitiveFields.@byte));
                Assert.Equal(2, primitives2.ReadUShort(PrimitiveFields.@ushort));
                Assert.Equal((uint) 3, primitives2.ReadUInt(PrimitiveFields.@uint));
                Assert.Equal((ulong) 4, primitives2.ReadULong(PrimitiveFields.@ulong));
                Assert.Equal(-1, primitives2.ReadFloat(PrimitiveFields.@float));
                Assert.Equal(-2, primitives2.ReadDouble(PrimitiveFields.@double));
                Assert.Equal(-3, primitives2.ReadDecimal(PrimitiveFields.@decimal));
                Assert.Equal('b', primitives2.ReadChar(PrimitiveFields.@char));
            }
        }

        [PrefixesFact]
        public void ShouldNotAllowToSkipVariableSizeFieldsByDefault()
        {
            var schema = new StructureSchema<MappedResults>()
                .Add<int>(MappedResults.View)
                .Add<string>(MappedResults.ReduceKey)
                .Add<string>(MappedResults.DocId);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.WriteStruct("structures/1", new Structure<MappedResults>(schema)
                    .Set(MappedResults.View, 1)
                    .Set(MappedResults.ReduceKey, "reduce")
                    .Set(MappedResults.DocId, "doc"));

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                var ex = Assert.Throws<InvalidOperationException>(() => tx.Root.WriteStruct("structures/1", new Structure<MappedResults>(schema)
                    .Set(MappedResults.View, 2)));

                Assert.Equal("Your structure schema defines variable size fields but you haven't set any. If you really want to skip those fields set AllowToSkipVariableSizeFields = true.", ex.Message);

                Assert.DoesNotThrow(() => tx.Root.WriteStruct("structures/1", new Structure<MappedResults>(schema)
                {
                    AllowToSkipVariableSizeFields = true
                }.Set(MappedResults.View, 2)));
            }

        }
    }
}
