using System;
using System.Collections.Generic;
using Sparrow.Binary;
using Sparrow.Server;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Data.RawData;
using Voron.Data.Tables;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public unsafe class RavenDB_27558_LowLevel(ITestOutputHelper output) : StorageTest(output)
    {
        // archived documents are written through CompressedDocsSchema while the rest of the collection uses DocsSchema,
        // so a single write transaction holds two Table objects (two ActiveRawDataSmallSection objects) over the same table
        [RavenFact(RavenTestCategory.Voron)]
        public void Two_tables_over_the_same_section_must_not_allocate_past_the_end_of_a_page()
        {
            using (var tx = Env.WriteTransaction())
            {
                CreateSchemas(tx, out var compressed, out _);
                compressed.Create(tx, "Items", 16);
                tx.Commit();
            }

            var expected = new Dictionary<string, byte[]>();
            var random = new Random(27558);
            long sectionPage;

            using (var tx = Env.WriteTransaction())
            {
                CreateSchemas(tx, out var compressed, out var plain);
                var a = tx.OpenTable(compressed, "Items");
                var b = tx.OpenTable(plain, "Items");
                Assert.NotSame(a, b);

                // b resolves the section header before a starts modifying it in this transaction
                sectionPage = b.ActiveDataSmallSection.PageNumber;

                // fill every page of the section through a, leaving less room than the value b is about to insert
                for (var i = 0; a.ActiveDataSmallSection.PageNumber == sectionPage; i++)
                {
                    Assert.True(i < 1000, "the section never filled up");
                    Insert(tx, a, $"filler/{i:D4}", 1900, random, expected);
                }

                Insert(tx, b, "big", 3000, random, expected);
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var numberOfPages = ((RawDataSmallSectionPageHeader*)tx.LowLevelTransaction.GetPage(sectionPage).Pointer)->NumberOfPages;
                for (var i = 1; i <= numberOfPages; i++)
                {
                    var page = (RawDataSmallPageHeader*)tx.LowLevelTransaction.GetPage(sectionPage + i).Pointer;
                    Assert.True(page->NextAllocation <= Constants.Storage.PageSize, $"page {page->PageNumber} allocated up to {page->NextAllocation}");
                }

                CreateSchemas(tx, out _, out var plain);
                var table = tx.OpenTable(plain, "Items");
                foreach (var (key, value) in expected)
                {
                    using (Slice.From(tx.Allocator, key, out var keySlice))
                    {
                        Assert.True(table.ReadByKey(keySlice, out var reader), key);
                        var ptr = reader.Read(2, out var size);
                        Assert.Equal(value, new ReadOnlySpan<byte>(ptr, size).ToArray());
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Entry_running_past_the_end_of_the_page_is_reported_as_corruption()
        {
            long id;
            using (var tx = Env.WriteTransaction())
            {
                CreateSchemas(tx, out _, out var plain);
                plain.Create(tx, "Items", 16);
                id = Insert(tx, tx.OpenTable(plain, "Items"), "item", 100, new Random(27558), new Dictionary<string, byte[]>());
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var page = tx.LowLevelTransaction.ModifyPage(id / Constants.Storage.PageSize);
                var sizes = (RawDataSection.RawDataEntrySizes*)(page.Pointer + id % Constants.Storage.PageSize);
                sizes->AllocatedSize = 8190; // 64 + 4 + 8190 runs past the 8192 byte page
                sizes->UsedSize = 8190;
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                CreateSchemas(tx, out _, out var plain);
                var table = tx.OpenTable(plain, "Items");
                using (Slice.From(tx.Allocator, "item", out var key))
                {
                    Assert.Throws<VoronUnrecoverableErrorException>(() => table.ReadByKey(key, out _));
                }
            }
        }

        private static long Insert(Transaction tx, Table table, string key, int valueSize, Random random, Dictionary<string, byte[]> expected)
        {
            var value = new byte[valueSize];
            random.NextBytes(value); // incompressible, so the compressed schema stores it as is
            expected[key] = value;

            using (table.Allocate(out var builder))
            using (Slice.From(tx.Allocator, key, out var keySlice))
            using (Slice.From(tx.Allocator, value.AsSpan(), out var valueSlice))
            {
                builder.Add(keySlice);
                builder.Add(Bits.SwapBytes((long)expected.Count));
                builder.Add(valueSlice);
                return table.Insert(builder);
            }
        }

        private static void CreateSchemas(Transaction tx, out TableSchema compressed, out TableSchema plain)
        {
            // same shape as the documents table: global pk tree, per-table etag fixed size tree
            Slice.From(tx.Allocator, "PK", ByteStringType.Immutable, out var pkName);
            Slice.From(tx.Allocator, "Etags", ByteStringType.Immutable, out var etagsName);
            var etags = new TableSchema.FixedSizeKeyIndexDef { Name = etagsName, IsGlobal = false, StartIndex = 1 };
            var key = new TableSchema.IndexDef { Name = pkName, IsGlobal = true, StartIndex = 0, Count = 1 };

            compressed = new TableSchema().CompressValues(etags, true).DefineFixedSizeIndex(etags).DefineKey(key);
            plain = new TableSchema().CompressValues(etags, false).DefineFixedSizeIndex(etags).DefineKey(key);
        }
    }
}
