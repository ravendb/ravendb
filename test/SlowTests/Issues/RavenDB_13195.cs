using System;
using System.Collections.Generic;
using FastTests.Voron;
using Sparrow;
using Sparrow.Server;
using Voron;
using Voron.Data.RawData;
using Voron.Data.Tables;
using Voron.Impl;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13195 : StorageTest
    {
        static RavenDB_13195()
        {
            using (StorageEnvironment.GetStaticContext(out ByteStringContext ctx))
            {
                Slice.From(ctx, "Local", ByteStringType.Immutable, out Local);
                Slice.From(ctx, "Etag", ByteStringType.Immutable, out Etag);
            }
        }
        
        private static readonly Slice Local;
        private static Slice Etag;

         [Fact]
        public unsafe void CanDeleteTableWithLargeValues()
        {
            TableSchema schema = new TableSchema();
            schema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1,
                Name = Etag,
                IsGlobal = true
            });

            schema.DefineIndex(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1,
                Name = Local,
                IsGlobal = false
            });

            using (var tx = Env.WriteTransaction())
            {
                schema.Create(tx, "first", 256);
                schema.Create(tx, "second", 256);

                var fst = tx.OpenTable(schema, "first");
                var snd = tx.OpenTable(schema, "second");

                var disposables = new List<IDisposable>();
                try
                {
                    for (var i = 0; i < 1000;)
                    {
                        var bytes = new byte[2 * RawDataSection.MaxItemSize + 100];

                        new Random(i).NextBytes(bytes);

                        disposables.Add(tx.Allocator.Allocate(bytes.Length, out var byteString));

                        var tvb1 = new TableValueBuilder();
                        var tvb2 = new TableValueBuilder();

                        tvb1.Add(i++);
                        tvb2.Add(i++);

                        fixed (byte* ptr = bytes)
                            Memory.Copy(byteString.Ptr, ptr, bytes.Length);

                        tvb1.Add(byteString.Ptr, bytes.Length);
                        tvb2.Add(byteString.Ptr, bytes.Length);

                        fst.Insert(tvb1);
                        snd.Insert(tvb2);
                    }
                    tx.Commit();
                }
                finally
                {
                    disposables.ForEach(x => x.Dispose());
                }
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.DeleteTable("first");
                tx.DeleteTable("second");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                Assert.Null(tx.LowLevelTransaction.RootObjects.Read("first"));
                Assert.Null(tx.LowLevelTransaction.RootObjects.Read("second"));
            }
        }
    }
}
