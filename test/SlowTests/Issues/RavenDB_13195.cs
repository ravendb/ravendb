﻿using System;
using FastTests.Voron;
using Sparrow;
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

            using (Transaction tx = Env.WriteTransaction())
            {
                schema.Create(tx, "first", 256);
                schema.Create(tx, "second", 256);

                Table fst = tx.OpenTable(schema, "first");
                Table snd = tx.OpenTable(schema, "second");

                for (var i = 0; i < 1000;)
                {
                    var bytes = new byte[2 * RawDataSection.MaxItemSize + 100];

                    new Random(i).NextBytes(bytes);

                    TableValueBuilder tvb1 = new TableValueBuilder();
                    TableValueBuilder tvb2 = new TableValueBuilder();

                    tvb1.Add(i++);
                    tvb2.Add(i++);

                    fixed (byte* ptr = bytes)
                    {
                        tvb1.Add(ptr, bytes.Length);
                        tvb2.Add(ptr, bytes.Length);
                    }

                    fst.Insert(tvb1);
                    snd.Insert(tvb2);
                }

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var firstTable = tx.OpenTable(schema, "first");

                firstTable.DeleteByPrimaryKey(Slices.BeforeAllKeys, x =>
                {
                    if (schema.Key.IsGlobal)
                    {
                        return firstTable.IsOwned(x.Reader.Id);
                    }

                    return true;
                });

                var secondTable = tx.OpenTable(schema, "first");

                secondTable.DeleteByPrimaryKey(Slices.BeforeAllKeys, x =>
                {
                    if (schema.Key.IsGlobal)
                    {
                        return secondTable.IsOwned(x.Reader.Id);
                    }

                    return true;
                });

                tx.Commit();
            }
        }
    }
}
