using System;
using Sparrow.Binary;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Xunit;

namespace FastTests.Voron.Tables
{
    // Mirrors the documents table shape: a primary key plus a global and a per-table
    // fixed-size etag index, then churns updates the way document overwrites do -
    // new monotonic etag, slightly different row size (forcing re-allocation and
    // raw-data-section defrag). Every update must find the previous etag in both
    // fixed size indexes.
    public unsafe class EtagIndexChurn(ITestOutputHelper output) : StorageTest(output)
    {
        private const int Docs = 50_000;

        [RavenFact(RavenTestCategory.Voron)]
        public void UpdatesMustAlwaysFindThePreviousEtag()
        {
            TableSchema schema;
            Slice allDocsEtags, collectionEtags;
            Slice.From(Allocator, "AllDocsEtags", out allDocsEtags);
            Slice.From(Allocator, "CollectionEtags", out collectionEtags);
            schema = new TableSchema()
                .DefineKey(new TableSchema.IndexDef { StartIndex = 0, Count = 1 })
                .DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef { Name = allDocsEtags, IsGlobal = true, StartIndex = 1 })
                .DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef { Name = collectionEtags, IsGlobal = false, StartIndex = 1 });

            long etag = 0;
            var rnd = new Random(42);
            var payload = new byte[1200];
            rnd.NextBytes(payload);
            var etags = new long[Docs];

            using (var tx = Env.WriteTransaction())
            {
                schema.Create(tx, "docs", 16);
                tx.Commit();
            }

            for (int lo = 0; lo < Docs; lo += 5000)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var table = tx.OpenTable(schema, "docs");
                    for (int i = lo; i < Math.Min(lo + 5000, Docs); i++)
                    {
                        etags[i] = ++etag;
                        using (table.Allocate(out TableValueBuilder b))
                        using (Slice.From(tx.Allocator, $"bench/{i:D8}", out var id))
                        {
                            b.Add(id);
                            b.Add(Bits.SwapBytes(etags[i]));
                            fixed (byte* p = payload)
                                b.Add(p, 800 + (i % 200));
                            table.Set(b);
                        }
                    }
                    tx.Commit();
                }
            }

            for (int txn = 0; txn < 150; txn++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var table = tx.OpenTable(schema, "docs");
                    for (int op = 0; op < 2000; op++)
                    {
                        int victim = rnd.Next(Docs);
                        etags[victim] = ++etag;
                        using (table.Allocate(out TableValueBuilder b))
                        using (Slice.From(tx.Allocator, $"bench/{victim:D8}", out var id))
                        {
                            b.Add(id);
                            b.Add(Bits.SwapBytes(etags[victim]));
                            fixed (byte* p = payload)
                                b.Add(p, 800 + rnd.Next(400)); // vary size -> new slot + defrag churn
                            table.Set(b); // overwrite: deletes old etag from BOTH indexes, adds the new
                        }
                    }
                    tx.Commit();
                }
            }
        }
    }
}
