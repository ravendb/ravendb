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
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues
{
    public unsafe class RavenDB_27558_LowLevel : StorageTest
    {
        public RavenDB_27558_LowLevel(ITestOutputHelper output) : base(output)
        {
        }

        // the ledger says a page has room, the page does not: the state a second Table object over the same section
        // (with a stale copy of the section header) used to leave behind, then allocate past the end of the page from
        [RavenFact(RavenTestCategory.Voron)]
        public void Ledger_claiming_more_space_than_the_page_has_must_not_allocate_past_the_end_of_the_page()
        {
            var expected = new Dictionary<string, byte[]>();
            var random = new Random(27558);
            long sectionPage;

            using (var tx = Env.WriteTransaction())
            {
                CreateSchemas(tx, out _, out var plain);
                plain.Create(tx, "Items", 16);
                var table = tx.OpenTable(plain, "Items");
                sectionPage = table.ActiveDataSmallSection.PageNumber;

                // four 1900 byte entries per page, no page has room for a fifth
                for (var i = 0; i < 4 * table.ActiveDataSmallSection.NumberOfPages; i++)
                    Insert(tx, table, $"filler/{i:D4}", 1900, random, expected);

                Assert.Equal(sectionPage, table.ActiveDataSmallSection.PageNumber);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.ModifyPage(sectionPage);
                var section = new RawDataSection(tx.LowLevelTransaction, sectionPage);
                var firstPage = (RawDataSmallPageHeader*)tx.LowLevelTransaction.GetPage(sectionPage + 1).Pointer;
                Assert.Equal(Constants.Storage.PageSize - firstPage->NextAllocation, section.AvailableSpace[0]);
                Assert.True(section.AvailableSpace[0] < 3000);
                section.AvailableSpace[0] = 4000; // the first page is full, but the ledger says otherwise
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                Assert.Equal(4000, new RawDataSection(tx.LowLevelTransaction, sectionPage).AvailableSpace[0]);
                CreateSchemas(tx, out _, out var plain);
                Insert(tx, tx.OpenTable(plain, "Items"), "big", 3000, random, expected);
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

        [RavenFact(RavenTestCategory.Voron)]
        public void Page_already_allocated_past_its_end_is_reported_as_corruption_instead_of_being_defragged()
        {
            long sectionPage;
            using (var tx = Env.WriteTransaction())
            {
                CreateSchemas(tx, out _, out var plain);
                plain.Create(tx, "Items", 16);
                var table = tx.OpenTable(plain, "Items");
                Insert(tx, table, "item", 100, new Random(27558), new Dictionary<string, byte[]>());
                sectionPage = table.ActiveDataSmallSection.PageNumber;
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                // the state a pre-fix overflow leaves behind: NextAllocation past the page, ledger wrapped to ~64K free
                tx.LowLevelTransaction.ModifyPage(sectionPage);
                var section = new RawDataSection(tx.LowLevelTransaction, sectionPage);
                var page = (RawDataSmallPageHeader*)tx.LowLevelTransaction.ModifyPage(sectionPage + 1).Pointer;
                page->NextAllocation = Constants.Storage.PageSize + 2943;
                section.AvailableSpace[0] = (ushort)(Constants.Storage.PageSize - page->NextAllocation);
                for (var i = 1; i < section.NumberOfPages; i++)
                    section.AvailableSpace[i] = 0; // force the defrag loop, where the wrapped ledger makes the corrupt page look free
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                CreateSchemas(tx, out _, out var plain);
                var table = tx.OpenTable(plain, "Items");
                var e = Assert.Throws<VoronUnrecoverableErrorException>(() => Insert(tx, table, "item/2", 100, new Random(27558), new Dictionary<string, byte[]>()));
                // the old DefragPage threw too, after modifying the page and running off its copy of it
                Assert.Contains("past the end of the page", e.Message);
                Assert.False(tx.LowLevelTransaction.IsDirty(sectionPage + 1));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void A_transaction_holds_one_table_per_name_whatever_the_schema()
        {
            using (var tx = Env.WriteTransaction())
            {
                CreateSchemas(tx, out _, out var plain);
                plain.Create(tx, "Items", 16);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                CreateSchemas(tx, out var compressed, out var plain);
                var table = tx.OpenTable(plain, "Items");
                Assert.Same(table, tx.OpenTable(compressed, "Items"));

                Insert(tx, table, "item", 100, new Random(27563), new Dictionary<string, byte[]>());
                Assert.Equal(1, tx.OpenTable(compressed, "Items").NumberOfEntries);

                tx.DeleteTable("Items");
                Assert.Null(tx.OpenTable(plain, "Items"));
            }
        }

        private static long Insert(Transaction tx, Table table, string key, int valueSize, Random random, Dictionary<string, byte[]> expected)
        {
            var value = new byte[valueSize];
            random.NextBytes(value);
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
