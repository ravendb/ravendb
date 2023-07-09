using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using Corax.Mappings;
using Corax.Utils;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;
using Version = Lucene.Net.Util.Version;

namespace FastTests.Corax;

public unsafe class DynamicFieldsTests : StorageTest
{
    public DynamicFieldsTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void WriteEmptyAsSimpleInDynamicField()
    {
        Assert.Fail("fix me");
        // const string fieldName = "Scope_0";
        // using ByteStringContext bsc = new(SharedMultipleUseFlag.None);
        //
        // using var _ = StorageEnvironment.GetStaticContext(out ByteStringContext ctx);
        // Slice.From(ctx, "A", ByteStringType.Immutable, out Slice aSlice);
        // Slice.From(ctx, "D", ByteStringType.Immutable, out Slice dSlice);
        //
        // using var builder = IndexFieldsMappingBuilder.CreateForWriter(false);
        // using var knownFields = builder.Build();
        // IndexEntryWriter writer = new(bsc, knownFields);
        //
        // writer.WriteDynamic(fieldName, Encoding.UTF8.GetBytes(""));
        // using var __ = writer.Finish(out ByteString element);
        // IndexEntryReader reader = new(element.Ptr, element.Length);
        //
        // var fieldReader = reader.GetFieldReaderFor(Encoding.UTF8.GetBytes(fieldName));
        // Assert.Equal(IndexEntryFieldType.Empty, fieldReader.Type);
    }

    [Fact]
    public void SimpleDynamicWrite()
    {
        using ByteStringContext bsc = new(SharedMultipleUseFlag.None);

        using IDisposable _ = StorageEnvironment.GetStaticContext(out ByteStringContext ctx);
        Slice.From(ctx, "A", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(ctx, "D", ByteStringType.Immutable, out Slice dSlice);

        // The idea is that GetField will return an struct we can use later on a loop (we just get it once).
        using IndexFieldsMapping knownFields = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice)
            .Build();

        using (var indexer = new IndexWriter(Env, knownFields))
        {
            using (var writer = indexer.Index("elements/1"))
            {
                writer.Write(0, Encoding.UTF8.GetBytes("1.001"), 1, 1.001);
                foreach (string term in new[] { "AAA", "BF", "CE" })
                {
                    writer.Write(1,Encoding.UTF8.GetBytes(term));
                }
                writer.Write(Constants.IndexWriter.DynamicField,"Name_123", Encoding.UTF8.GetBytes("Oren"));
                writer.Write(Constants.IndexWriter.DynamicField,"Name_433", Encoding.UTF8.GetBytes("Eini"));
                writer.Write(Constants.IndexWriter.DynamicField,"Scope_0", Encoding.UTF8.GetBytes(""));
                writer.WriteNull(Constants.IndexWriter.DynamicField, "Scope_1");
                foreach (string term in new[] { "AAA", "GBP", "CE" })
                {
                    writer.Write(Constants.IndexWriter.DynamicField, "Items_UK", Encoding.UTF8.GetBytes(term));
                }

                writer.Write(Constants.IndexWriter.DynamicField, "Age_0", Encoding.UTF8.GetBytes("30.31"), 30, 30.31);
                writer.Write(Constants.IndexWriter.DynamicField,"Age_1", Encoding.UTF8.GetBytes("10"), 10, 10);
            }

            indexer.PrepareAndCommit();
        }

        using (var searcher = new IndexSearcher(Env, knownFields))
        {
            Span<long> ids = new long[16];
            var entries = searcher.GreaterThanQuery(searcher.FieldMetadataBuilder("Age_1"), 5L);
            Assert.Equal(1, entries.Fill(ids));
        }
        
        using (var searcher = new IndexSearcher(Env, knownFields))
        {
            Span<long> ids = new long[16];
            var entries = searcher.TermQuery("Scope_0", Constants.EmptyString);
            Assert.Equal(1, entries.Fill(ids));
        }
        
        using (var searcher = new IndexSearcher(Env, knownFields))
        {
            Span<long> ids = new long[16];
            var entries = searcher.TermQuery("Scope_1", Constants.NullValue);
            Assert.Equal(1, entries.Fill(ids));
        }
        
        using (var searcher = new IndexSearcher(Env, knownFields))
        {
            Span<long> ids = new long[16];
            var entries = searcher.TermQuery("Items_UK", "GBP");
            Assert.Equal(1, entries.Fill(ids));
        }
        
        using (var searcher = new IndexSearcher(Env, knownFields))
        {
            Span<long> ids = new long[16];
            var entries = searcher.TermQuery("NotExists", Constants.EmptyString);
            Assert.Equal(0, entries.Fill(ids));
        }
    }
    
    [Fact]
    public void WillDeleteDynamicReferences()
    {
        using IDisposable __ = StorageEnvironment.GetStaticContext(out ByteStringContext ctx);
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(ctx, "Name", ByteStringType.Immutable, out Slice dSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice);
        var fields = builder.Build();

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

        using (var writer = new IndexWriter(Env, fields))
        {
            using (var entry = writer.Update("users/1"u8))
            {
                entry.Write(0, "users/1"u8);
                entry.Write(1, "Oren"u8);
                entry.Write(Constants.IndexWriter.DynamicField,"Nick", "Ayende"u8);
            }
            writer.PrepareAndCommit();
        }


        using (var writer = new IndexWriter(Env, fields))
        {
            writer.TryDeleteEntry("users/1");
            writer.PrepareAndCommit();
        }

        using (var searcher = new IndexSearcher(Env, fields))
        {
            Span<long> ids = stackalloc long[16];
            var read = searcher.TermQuery("Nick", "Ayende").Fill(ids);
            Assert.Equal(0, read);
        }
    }
    
    [Fact]
    public void WillDeleteDynamicReferencesWithOutOfOrderRepeats()
    {
        using IDisposable __ = StorageEnvironment.GetStaticContext(out ByteStringContext ctx);
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(ctx, "Name", ByteStringType.Immutable, out Slice dSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice,  LuceneAnalyzerAdapter.Create(new RavenStandardAnalyzer(Version.LUCENE_29)));
        var fields = builder.Build();

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

        using (var writer = new IndexWriter(Env, fields))
        {
            using (var entry = writer.Update("users/1"u8))
            {
                entry.Write(0, "users/1"u8);
                entry.Write(1, "Oren"u8);
                entry.Write(Constants.IndexWriter.DynamicField,"Nick", "Ayende"u8);
                entry.Write(Constants.IndexWriter.DynamicField,"Name", "Eini Oren"u8);
            }
            
            writer.PrepareAndCommit();
        }


        using (var writer = new IndexWriter(Env, fields))
        {
            writer.TryDeleteEntry("users/1");
            writer.PrepareAndCommit();
        }

        using (var searcher = new IndexSearcher(Env, fields))
        {
            Span<long> ids = stackalloc long[16];
            var read = searcher.TermQuery("Name", "Oren").Fill(ids);
            Assert.Equal(0, read);
        }
    }

    
    [Fact]
    public void DynamicFieldWithSameNameOfStatic()
    {
        using IDisposable __ = StorageEnvironment.GetStaticContext(out ByteStringContext ctx);
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(ctx, "Name", ByteStringType.Immutable, out Slice dSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice);
        var fields = builder.Build();

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

        using (var writer = new IndexWriter(Env, fields))
        {
            using (var entry = writer.Update("users/1"u8))
            {
                entry.Write(0, "users/1"u8);
                entry.Write(1, "Oren"u8);
                entry.Write(Constants.IndexWriter.DynamicField,"Name", "Oren"u8);
            }
            writer.PrepareAndCommit();
        }


        using (var writer = new IndexWriter(Env, fields))
        {
            writer.TryDeleteEntry("users/1");
            writer.PrepareAndCommit();
        }

        using (var searcher = new IndexSearcher(Env, fields))
        {
            Span<long> ids = stackalloc long[16];
            var read = searcher.TermQuery("Name", "Oren").Fill(ids);
            Assert.Equal(0, read);
        }
    }
    
    
    [Fact]
    public void MixingStaticAndDynamicFieldsCorax()
    {
        using IDisposable __ = StorageEnvironment.GetStaticContext(out ByteStringContext ctx);
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(ctx, "Name", ByteStringType.Immutable, out Slice dSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice);
        var fields = builder.Build();

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using (var writer = new IndexWriter(Env, fields))
        {
            using (var entry = writer.Index("users/1"))
            {
                entry.Write(0, "users/1"u8);
                entry.Write(1, "Oren"u8);
                entry.Write(Constants.IndexWriter.DynamicField, "Rank", "U"u8);
            }

            writer.PrepareAndCommit();
        }

        using (var writer = new IndexWriter(Env, fields))
        {
            using (var entry = writer.Update("users/1"u8))
            {
                entry.Write(0, "users/1"u8);
                entry.Write(1, "Oren"u8);
                entry.Write(Constants.IndexWriter.DynamicField,"Name", "Oren"u8);
            }
            writer.PrepareAndCommit();
        }


        using (var writer = new IndexWriter(Env, fields))
        {
            using (var entry = writer.Update("users/1"u8))
            {
                entry.Write(0, "users/1"u8);
                entry.Write(1, "Eini"u8);
                entry.Write(Constants.IndexWriter.DynamicField,"Name", "Eini"u8);
            }
           
            writer.PrepareAndCommit();
        }

        using (var searcher = new IndexSearcher(Env, fields))
        {
            Span<long> ids = stackalloc long[16];
            var read = searcher.TermQuery("Name", "Oren").Fill(ids);
            Assert.Equal(0, read);
        }
        
        using (var searcher = new IndexSearcher(Env, fields))
        {
            Span<long> ids = stackalloc long[16];
            var read = searcher.TermQuery("Name", "Eini").Fill(ids);
            Assert.Equal(1, read);
        }
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(48.666708, -4.333999, "gbsuv7s04")]
    [InlineData(53.015261, 18.611487, "u3mjxe0kr")]
    public void CanIndexReadAndDeleteLongLatSpatialDynamically(double latitude, double longitude, string geohash)
    {
        using IndexFieldsMapping fields = PrepareSpatial();

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using (var writer = new IndexWriter(Env, fields))
        {
            using (var builder = writer.Index(IdString))
            {
                builder.Write(0, Encodings.Utf8.GetBytes(IdString));
                var spatialEntry = new CoraxSpatialPointEntry(latitude, longitude, geohash);
                builder.WriteSpatial(Constants.IndexWriter.DynamicField,"Coordinates_Home", spatialEntry);

            }
            writer.PrepareAndCommit();
        }

        for (int i = 0; i < geohash.Length; ++i)
        {
            var partialGeohash = geohash.Substring(0, i + 1);
            using (var searcher = new IndexSearcher(Env, fields))
            {
                Span<long> ids = new long[16];
                var entries = searcher.TermQuery("Coordinates_Home", partialGeohash);
                Assert.Equal(1, entries.Fill(ids));
                Page p = default;
                var reader = searcher.GetEntryTermsReader(ids[0], ref p);
                Assert.True(reader.MoveNextSpatial());
                Assert.Equal(reader.Latitude, latitude);
                Assert.Equal(reader.Longitude, longitude);
            }
        }

        using (var writer = new IndexWriter(Env, fields))
        {
            writer.TryDeleteEntry(IdString);
            writer.PrepareAndCommit();
        }

        using (var searcher = new IndexSearcher(Env, fields))
        {
            Span<long> ids = new long[16];
            var entries = searcher.TermQuery("Coordinates_Home", geohash);
            Assert.Equal(0, entries.Fill(ids));
        }
    }

    const string IdString = "entry-1";

    private static IndexFieldsMapping PrepareSpatial()
    {
        using IDisposable __ = StorageEnvironment.GetStaticContext(out ByteStringContext ctx);
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(ctx, "D", ByteStringType.Immutable, out Slice dSlice);

        // The idea is that GetField will return an struct we can use later on a loop (we just get it once).
        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice);
        return builder.Build();
    }

    [Theory]
    [InlineData(4, new double[]{ -10.5, 12.4, -123D, 53}, new double[]{-52.123, 23.32123, 52.32423, -42.1235})]
    public unsafe void WriteAndReadSpatialListDynamically(int size, double[] lat, double[] lon)
    {
        Assert.Fail("fix me");
        // using IndexFieldsMapping fields = PrepareSpatial();
        // using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        //
        // var entryBuilder = new IndexEntryWriter(bsc, fields);
        // entryBuilder.Write(0, Encodings.Utf8.GetBytes("item/1"));
        // Span<CoraxSpatialPointEntry> _points = new CoraxSpatialPointEntry[size];
        // for (int i = 0; i < size; ++i)
        //     _points[i] = new CoraxSpatialPointEntry(lat[i], lon[i], Spatial4n.Util.GeohashUtils.EncodeLatLon(lat[i], lon[i], 9));
        // entryBuilder.WriteSpatialDynamic("CoordinatesIndex", _points);
        // using var _ = entryBuilder.Finish(out var buffer);
        //
        // var reader = new IndexEntryReader(buffer.Ptr, buffer.Length);
        //
        // var fieldReader = reader.GetFieldReaderFor(Encoding.UTF8.GetBytes("CoordinatesIndex"));
        //
        // Assert.True(fieldReader.TryReadManySpatialPoint(out SpatialPointFieldIterator iterator));
        // List<CoraxSpatialPointEntry> entriesInIndex = new();
        //
        // while (iterator.ReadNext())
        // {
        //     entriesInIndex.Add(iterator.CoraxSpatialPointEntry);
        // }        
        //
        // Assert.Equal(size, entriesInIndex.Count);
        //
        // for (int i = 0; i < size; ++i)
        // {
        //     var entry = new CoraxSpatialPointEntry(lat[i], lon[i], Spatial4n.Util.GeohashUtils.EncodeLatLon(lat[i], lon[i], 9));
        //
        //     var entryFromBuilder = entriesInIndex.Single(p => p.Geohash == entry.Geohash);
        //     Assert.Equal(entry.Latitude, entryFromBuilder.Latitude);
        //     Assert.Equal(entry.Longitude, entryFromBuilder.Longitude);
        //     entriesInIndex.Remove(entry);
        // }
        //
        // Assert.Empty(entriesInIndex);
    }
    
     
    [Fact]
    public void MixingStaticAndDynamicFieldsCorax3()
    {
        using IDisposable __ = StorageEnvironment.GetStaticContext(out ByteStringContext ctx);
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(ctx, "Name", ByteStringType.Immutable, out Slice dSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice);
        var fields = builder.Build();

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using (var writer = new IndexWriter(Env, fields))
        {
            using (var entryBuilder = writer.Index("users/1"))
            {
                entryBuilder.Write(0, "users/1"u8);
                entryBuilder.Write(1, "Oren"u8);
                entryBuilder.Write(Constants.IndexWriter.DynamicField,"Rank", "U"u8);
            }
            writer.PrepareAndCommit();
        }
        
        using (var writer = new IndexWriter(Env, fields))
        {
            using (var entryBuilder = writer.Update("users/1"u8))
            {
                entryBuilder.Write(0, "users/1"u8);
                entryBuilder.Write(1, "Oren"u8);
                entryBuilder.Write(Constants.IndexWriter.DynamicField,"Name", "Maciej"u8);
            }
          
            writer.PrepareAndCommit();
        }

        using (var searcher = new IndexSearcher(Env, fields))
        {
            Span<long> ids = stackalloc long[16];
            var read = searcher.TermQuery("Name", "Maciej").Fill(ids);
            Assert.Equal(1, read);
        }
        
        using (var writer = new IndexWriter(Env, fields))
        {
            writer.TryDeleteEntry("users/1");
            writer.PrepareAndCommit();
        }

        using (var searcher = new IndexSearcher(Env, fields))
        {
            Span<long> ids = stackalloc long[16];
            var read = searcher.TermQuery("Name", "Maciej").Fill(ids);
            Assert.Equal(0, read);
        }
    }
    
    [Fact]
    public void MixingStaticAndDynamicFieldsCorax2()
    {
        using IDisposable __ = StorageEnvironment.GetStaticContext(out ByteStringContext ctx);
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(ctx, "Name", ByteStringType.Immutable, out Slice dSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice);
        var fields = builder.Build();

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using (var writer = new IndexWriter(Env, fields))
        {
            using (var entryBuilder = writer.Update("users/1"u8))
            {
                entryBuilder.Write(0, "users/1"u8);
                entryBuilder.Write(1, "Oren"u8);
                entryBuilder.Write(Constants.IndexWriter.DynamicField,"Rank", "U"u8);

            }
            writer.PrepareAndCommit();
        }
        
        using (var writer = new IndexWriter(Env, fields))
        {
            using (var entryBuilder = writer.Update("users/1"u8))
            {
                entryBuilder.Write(0, "users/1"u8);
                entryBuilder.Write(1, "Oren"u8);
                entryBuilder.Write(Constants.IndexWriter.DynamicField,"Name", "Maciej"u8);

            }
            writer.PrepareAndCommit();
        }

        using (var searcher = new IndexSearcher(Env, fields))
        {
            Span<long> ids = stackalloc long[16];
            var read = searcher.TermQuery("Name", "Maciej").Fill(ids);
            Assert.Equal(1, read);
        }
     
        using (var writer = new IndexWriter(Env, fields))
        {
            using (var entry = writer.Update("users/1"u8))
            {
                entry.Write(0, "users/1"u8);
                entry.Write(1, "Eini"u8);
                entry.Write(Constants.IndexWriter.DynamicField,"Name", "Eini"u8);
            }
            writer.PrepareAndCommit();
        }
        
        using (var searcher = new IndexSearcher(Env, fields))
        {
            Span<long> ids = stackalloc long[16];
            var read = searcher.TermQuery("Name", "Maciej").Fill(ids);
            Assert.Equal(0, read);
            read = searcher.TermQuery("Name", "Eini").Fill(ids);
            Assert.Equal(1, read);
        }
    }
}
