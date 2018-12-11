using System;
using FastTests.Voron;
using Sparrow;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;
using Xunit;

namespace SlowTests.Voron
{
    public class DeletionFromMultiTableWithGlobalIndex : StorageTest
    {
        static DeletionFromMultiTableWithGlobalIndex()
        {
            using (StorageEnvironment.GetStaticContext(out ByteStringContext ctx))
            {
                Slice.From(ctx, "Global", ByteStringType.Immutable, out Global);
                Slice.From(ctx, "Local", ByteStringType.Immutable, out Local);
                Slice.From(ctx, "Etag", ByteStringType.Immutable, out Etag);
            }
        }

        private static readonly Slice Global;
        private static readonly Slice Local;
        private static Slice Etag;

        [Fact]
        public unsafe void ShouldNotError()
        {
            TableSchema schema = new TableSchema();
            schema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1,
                Name = Etag,
                IsGlobal = true
            });

            schema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = 1,
                Count = 1,
                Name = Global,
                IsGlobal = true
            });

            schema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = 2,
                Count = 1,
                Name = Local,
                IsGlobal = false
            });

            using (Transaction tx = Env.WriteTransaction())
            {
                schema.Create(tx, "test", 256);

                Table fst = tx.OpenTable(schema, "test");

                schema.Create(tx, "snd", 256);

                for (int i = 0; i < 20_000; i ++)
                {
                    TableValueBuilder tvb = new TableValueBuilder();
                    tvb.Add(i);
                    tvb.Add(0);
                    tvb.Add(i);

                    fst.Insert(tvb);
                }

                tx.Commit();
            }

            using (Transaction tx = Env.WriteTransaction())
            {
                Table snd = tx.OpenTable(schema, "snd");
                Table fst = tx.OpenTable(schema, "test");
                

                for (int i = 1; i < 20_000; i += 2)
                {
                    using (Slice.From(tx.Allocator, BitConverter.GetBytes(i), out var key))
                    {
                        var a  = fst.DeleteForwardFrom(schema.Indexes[Etag], key, true, 1);
                        Assert.True(1  == a, $"{a} on {i}");
                    }
                    
                    snd.Insert(new TableValueBuilder
                    {
                        i,
                        0,
                        i
                    });
                }

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                
                Table snd = tx.OpenTable(schema, "snd");

                for (int i = 1; i < 20_000; i+=2)
                {
                    using (Slice.From(tx.Allocator, BitConverter.GetBytes(i), out var key))
                    {
                        var a  = snd.DeleteForwardFrom(schema.Indexes[Etag], key, true, 1);
                        Assert.True(1  == a, $"{a} on {i}");
                    }
                }

            }
        }
        
        
        [Fact]
        public unsafe void SameTransaction()
        {
            TableSchema schema = new TableSchema();
            schema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = 1,
                Count = 1,
                Name = Global,
                IsGlobal = true
            });

            schema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = 2,
                Count = 1,
                Name = Local,
                IsGlobal = false
            });

            using (Transaction tx = Env.WriteTransaction())
            {
                schema.Create(tx, "test", 256);

                Table tbl = tx.OpenTable(schema, "test");

                schema.Create(tx, "snd", 256);

                Table snd = tx.OpenTable(schema, "snd");


                for (int i = 0; i < 20_000; i+=2)
                {
                    var tvb = new TableValueBuilder();
                    tvb.Add(i);
                    tvb.Add(0);
                    tvb.Add(i);

                    tbl.Insert(tvb);
                }

                for (int i = 1; i < 20_000; i += 2)
                {
                    TableValueBuilder tvb = new TableValueBuilder();
                    tvb.Add(i);
                    tvb.Add(0);
                    tvb.Add(i);

                    snd.Insert(tvb);
                }

                tx.Commit();
            }
            
            using (var tx = Env.WriteTransaction())
            {
                
                Table snd = tx.OpenTable(schema, "snd");

                var a  = snd.DeleteForwardFrom(schema.Indexes[Local], Slices.BeforeAllKeys, false, long.MaxValue);
                Assert.Equal(10_000, a);

            }
        }
    }
}
