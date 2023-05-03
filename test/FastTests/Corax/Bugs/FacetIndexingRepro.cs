using System;
using System.IO;
using Corax;
using Corax.Mappings;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Voron.Data.Containers;
using Voron.Impl;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class FacetIndexingRepro : StorageTest
{

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
    public void CanSuccessfullyIndexData()
    {
        using var stream = typeof(PostingListAddRemoval).Assembly.GetManifestResourceStream("FastTests.Corax.Bugs." + "index-log.bin");
        using var br = new BinaryReader(stream);

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

        using (var wtx = Env.WriteTransaction())
        {
            var iw = new IndexWriter(wtx, CreateKnownFields(bsc));
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
                    iw = new IndexWriter(wtx, CreateKnownFields(bsc));
                    continue;
                }

                int len = br.Read7BitEncodedInt();
                var buffer = br.ReadBytes(len);
                iw.Index(id, buffer);

            }
        }
    }

    private IndexFieldsMapping CreateKnownFields(ByteStringContext bsc)
    {
        Slice.From(bsc, "id()", ByteStringType.Immutable, out Slice id);
        Slice.From(bsc, "Manufacturer", ByteStringType.Immutable, out Slice Manufacturer);
        Slice.From(bsc, "Model", ByteStringType.Immutable, out Slice Model);
        Slice.From(bsc, "Cost", ByteStringType.Immutable, out Slice Cost);
        Slice.From(bsc, "DateOfListing", ByteStringType.Immutable, out Slice DateOfListing);
        Slice.From(bsc, "Megapixels", ByteStringType.Immutable, out Slice Megapixels);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, id)
            .AddBinding(1, Manufacturer)
            .AddBinding(2, Model)
            .AddBinding(3, Cost)
            .AddBinding(4, DateOfListing)
            .AddBinding(5, Megapixels);

        return builder.Build();
    }

    public FacetIndexingRepro(ITestOutputHelper output) : base(output)
    {
    }
}
