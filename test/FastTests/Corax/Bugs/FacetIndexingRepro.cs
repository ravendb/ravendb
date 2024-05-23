using System.IO;
using Corax;
using Corax.Querying;
using Corax.Mappings;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Impl;
using Xunit;
using Xunit.Abstractions;
using Encoding = System.Text.Encoding;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace FastTests.Corax.Bugs;

public class FacetIndexingRepro : StorageTest
{

    [Fact]
    public void CanHandleRecompressionOfEntryTerms()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        Slice.From(bsc, "id()", out var id);
        var fields = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, id)
            .Build();

        using var wtx = Env.WriteTransaction();

        var fieldsTree = wtx.CreateTree(Constants.IndexWriter.FieldsSlice);
        CompactTree idTree = fieldsTree.CompactTreeFor(id);

        using var iw = new IndexWriter(wtx, fields, SupportedFeatures.All);
        string entryKey = "users/00000001";
        long entryId;
        using (var builder = iw.Index("entryKey"))
        {
            builder.Write(0, Encoding.UTF8.GetBytes(entryKey));
            entryId = builder.EntryId;
            builder.EndWriting();
        }
        iw.Commit();
        
        {
            var searcher = new IndexSearcher(wtx, fields);
            TermsReader termsReader = searcher.TermsReaderFor(id);
            Assert.Equal(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(entryId), termsReader.GetTermFor(entryId));
        }
        
        // here we force it to take place
        idTree.TryRemove("missing-value", out _);

        {
            var searcher = new IndexSearcher(wtx, fields);
        
            TermsReader termsReader = searcher.TermsReaderFor(id);
            Assert.Equal(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(entryId), termsReader.GetTermFor(entryId));
        }
    }

    [Fact]
    public void CanAddAndRemoveItems()
    {
        using (var wtx = Env.WriteTransaction())
        {
            var c = wtx.OpenContainer("test");

            var items = Items;
            for (int i = 0; i < items.Length; i++)
            {
                switch (items[i].Item1)
                {
                    case '+':
                        Container.Allocate(wtx.LowLevelTransaction, c, items[i].Item2, out _);
                        break;
                    case '-':
                        Container.Delete(wtx.LowLevelTransaction, c, items[i].Item2);
                        break;
                }
            }
            wtx.Commit();
        }
    }

    private (char, int)[] Items = new[]
    {
        ('+', 32), ('+', 64), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32),
        ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('-', 16416), ('+', 64), ('-', 16424), ('+', 96),
        ('-', 16432), ('+', 64), ('-', 16440), ('+', 64), ('-', 16448), ('+', 96), ('-', 16456), ('+', 96), ('-', 16464), ('+', 96), ('-', 16472), ('+', 64),
        ('-', 16480), ('+', 64), ('-', 16488), ('+', 96), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32),
        ('-', 16528), ('+', 64), ('-', 16536), ('+', 64), ('-', 16584), ('+', 64), ('-', 16560), ('+', 64), ('-', 16520), ('+', 64), ('-', 16592), ('+', 64),
        ('-', 16544), ('+', 64), ('-', 16416), ('+', 96), ('-', 16424), ('+', 128), ('-', 16432), ('+', 96), ('-', 16440), ('+', 96), ('-', 16448), ('+', 128),
        ('-', 16456), ('+', 128), ('-', 16472), ('+', 128), ('-', 16480), ('+', 96), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32),
        ('+', 32), ('+', 32), ('-', 16520), ('+', 96), ('-', 16568), ('+', 64), ('-', 16536), ('+', 96), ('-', 16552), ('+', 64), ('-', 16576), ('+', 64),
        ('-', 16416), ('+', 128), ('-', 16432), ('+', 128), ('-', 16440), ('+', 128), ('-', 16448), ('+', 160), ('-', 16456), ('+', 160), ('-', 16464), ('+', 160),
        ('-', 16472), ('+', 160), ('-', 16480), ('+', 128), ('-', 16488), ('+', 128), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32),
        ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('+', 32), ('-', 16592), ('+', 96), ('-', 16528), ('+', 96), ('-', 16584), ('+', 96), ('-', 16416), ('+', 160),
        ('-', 16424), ('+', 192), ('-', 16432), ('+', 160), ('-', 16440), ('+', 160), ('-', 16448), ('+', 192), ('-', 16456), ('+', 192), ('-', 16464), ('+', 192),
        ('-', 16472), ('+', 192), ('-', 16488), ('+', 160), ('+', 32),
    };

    [Fact]
    public unsafe void ShouldNotCorrupt()
    {
        using var stream = typeof(PostingListAddRemoval).Assembly.GetManifestResourceStream("FastTests.Corax.Bugs." + "index-corrupt-log.bin");
        using var br = new BinaryReader(stream);

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false);
        var fieldsCount = br.ReadInt32();
        for (int i = 0; i < fieldsCount; i++)
        {
            var name = br.ReadString();
            builder.AddBinding(i, name);
        }

        var fields = builder.Build();
        int txns = 0;
        int items = 0;
        var wtx = Env.WriteTransaction();
        try
        {
            var iw = new IndexWriter(wtx, fields, SupportedFeatures.All);
            while (true)
            {
                string id;
                try
                {
                    id = br.ReadString();
                }
                catch (EndOfStreamException)
                {
                    iw.Commit();
                    iw.Dispose();
                    wtx.Commit();
                    wtx.Dispose();
                    break;
                }

                if (id == "!Commit!")
                {
                    FlushIndexAndRenewWriteTransaction();
                    continue;
                }

                using(var indexEntryBuilder = iw.Index(id))
                {
                    for (int i = 0; i < fieldsCount; i++)
                    {
                       int len = br.Read7BitEncodedInt();
                       var s = br.ReadBytes(len);
                       indexEntryBuilder.Write(i, s);
                    }
                    indexEntryBuilder.EndWriting();
                }
                
                items++;
            }

            
            using (var rtx = Env.ReadTransaction())
                QueryAll(rtx);
            
            
            void FlushIndexAndRenewWriteTransaction()
            {
                iw.Commit();
                iw.Dispose();
                wtx.Commit();
                wtx.Dispose();

                using (var rtx = Env.ReadTransaction())
                    QueryAll(rtx);

                txns++;
                items = 0;
                wtx = Env.WriteTransaction();
                iw = new IndexWriter(wtx, fields, SupportedFeatures.All);
            }
        }
        finally
        {
            wtx.Dispose();
        }

        void QueryAll(Transaction rtx)
        {
            var matches = new long[1024];
            for (int i = 0; i < fieldsCount; i++)
            {
                var searcher = new IndexSearcher(rtx, fields);
                var field = FieldMetadata.Build(fields.GetByFieldId(i).FieldName, default, i, default, default);
                var sort = searcher.OrderBy(searcher.AllEntries(), new OrderMetadata(field, true, MatchCompareFieldType.Sequence));
                while (sort.Fill(matches) != 0)
                {
                }
            }
        }

    }
    
    [Fact]
    public unsafe void CanSuccessfullyIndexData()
    {
        using var stream = typeof(PostingListAddRemoval).Assembly.GetManifestResourceStream("FastTests.Corax.Bugs." + "index-log.bin");
        using var br = new BinaryReader(stream);

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

        Slice.From(bsc, "id()", ByteStringType.Immutable, out Slice id1);
        Slice.From(bsc, "Manufacturer", ByteStringType.Immutable, out Slice Manufacturer);
        Slice.From(bsc, "Model", ByteStringType.Immutable, out Slice Model);
        Slice.From(bsc, "Cost", ByteStringType.Immutable, out Slice Cost);
        Slice.From(bsc, "DateOfListing", ByteStringType.Immutable, out Slice DateOfListing);
        Slice.From(bsc, "Megapixels", ByteStringType.Immutable, out Slice Megapixels);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, id1)
            .AddBinding(1, Manufacturer)
            .AddBinding(2, Model)
            .AddBinding(3, Cost)
            .AddBinding(4, DateOfListing)
            .AddBinding(5, Megapixels);

        IndexFieldsMapping indexFieldsMapping = builder.Build();
        using (var wtx = Env.WriteTransaction())
        {
            var iw = new IndexWriter(wtx, indexFieldsMapping, SupportedFeatures.All);
            while (true)
            {
                string id;
                try
                {
                    id = br.ReadString();
                }
                catch (EndOfStreamException)
                {

                    iw.Commit();
                    iw.Dispose();
                    break;
                }

                if (id == "!Commit!")
                {
                    iw.Commit();
                    iw.Dispose();
                    iw = new IndexWriter(wtx, indexFieldsMapping, SupportedFeatures.All);
                    continue;
                }

                using var indexEntryBuilder = iw.Index(id);
                {
                    for (int i = 0; i < indexFieldsMapping.Count; i++)
                    {
                        var len = br.Read7BitEncodedInt();
                        var b = br.ReadBytes(len);
                        indexEntryBuilder.Write(i, b);
                    }
                }

            }
        }
    }
    public FacetIndexingRepro(ITestOutputHelper output) : base(output)
    {
    }
}

