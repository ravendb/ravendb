using System;
using System.Collections.Generic;
using System.Text;
using Corax;
using Corax.Querying;
using Corax.Mappings;
using Corax.Utils;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;
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
        const string fieldName = "Scope_0";
        using ByteStringContext bsc = new(SharedMultipleUseFlag.None);
        
        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false);
        builder.AddBinding(0, "Id");
        using var knownFields = builder.Build();
        long entryId;
        using (var indexWriter = new IndexWriter(Env, knownFields, SupportedFeatures.All))
        {
            indexWriter.UpdateDynamicFieldsMapping(IndexFieldsMappingBuilder.CreateForWriter(true)
                .AddBinding(Constants.IndexWriter.DynamicField, fieldName, shouldStore:true)
                .Build()
            );
            using (var writer = indexWriter.Index("users/1"))
            {
                writer.Write(Constants.IndexWriter.DynamicField, fieldName, Encoding.UTF8.GetBytes(""));
                writer.Write(0, "users/1"u8);
                entryId = writer.EntryId;
                writer.EndWriting();
            }
            indexWriter.Commit();
        }
        
        using (var indexSearcher = new IndexSearcher(Env, knownFields))
        {
            Page p = default;
            var reader = indexSearcher.GetEntryTermsReader(entryId, ref p);
            long fieldRootPage = indexSearcher.FieldCache.GetLookupRootPage(fieldName);
            Assert.True(reader.FindNext(fieldRootPage));
            Assert.Equal(Constants.EmptyStringSlice.AsSpan().ToArray(), reader.Current.Decoded().ToArray());
            reader.Reset();
            Assert.True(reader.FindNextStored(fieldRootPage));
            Assert.Equal(Array.Empty<byte>(), reader.StoredField.Value.ToSpan().ToArray());
        }
    }

    [Fact]
    public void SimpleDynamicWrite()
    {
        using ByteStringContext bsc = new(SharedMultipleUseFlag.None);

        Slice.From(bsc, "A", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(bsc, "D", ByteStringType.Immutable, out Slice dSlice);

        // The idea is that GetField will return an struct we can use later on a loop (we just get it once).
        using IndexFieldsMapping knownFields = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice)
            .Build();

        using (var indexer = new IndexWriter(Env, knownFields, SupportedFeatures.All))
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
                writer.EndWriting();
            }

            indexer.Commit();
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
            var entries = searcher.TermQuery("Scope_1", null);
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
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        Slice.From(bsc, "Id", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(bsc, "Name", ByteStringType.Immutable, out Slice dSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice);
        var fields = builder.Build();


        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var entry = writer.Update("users/1"u8))
            {
                entry.Write(0, "users/1"u8);
                entry.Write(1, "Oren"u8);
                entry.Write(Constants.IndexWriter.DynamicField,"Nick", "Ayende"u8);
                entry.EndWriting();
            }
            writer.Commit();
        }


        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            writer.TryDeleteEntry("users/1");
            writer.Commit();
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
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        Slice.From(bsc, "Id", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(bsc, "Name", ByteStringType.Immutable, out Slice dSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice,  LuceneAnalyzerAdapter.Create(new RavenStandardAnalyzer(Version.LUCENE_29)));
        var fields = builder.Build();


        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var entry = writer.Update("users/1"u8))
            {
                entry.Write(0, "users/1"u8);
                entry.Write(1, "Oren"u8);
                entry.Write(Constants.IndexWriter.DynamicField,"Nick", "Ayende"u8);
                entry.Write(Constants.IndexWriter.DynamicField,"Name", "Eini Oren"u8);
                entry.EndWriting();
            }
            
            writer.Commit();
        }


        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            writer.TryDeleteEntry("users/1");
            writer.Commit();
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
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        Slice.From(bsc, "Id", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(bsc, "Name", ByteStringType.Immutable, out Slice dSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice);
        var fields = builder.Build();


        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var entry = writer.Update("users/1"u8))
            {
                entry.Write(0, "users/1"u8);
                entry.Write(1, "Oren"u8);
                entry.Write(Constants.IndexWriter.DynamicField,"Name", "Oren"u8);
                entry.EndWriting();
            }
            writer.Commit();
        }


        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            writer.TryDeleteEntry("users/1");
            writer.Commit();
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
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        Slice.From(bsc, "Id", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(bsc, "Name", ByteStringType.Immutable, out Slice dSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice);
        var fields = builder.Build();

        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var entry = writer.Index("users/1"))
            {
                entry.Write(0, "users/1"u8);
                entry.Write(1, "Oren"u8);
                entry.Write(Constants.IndexWriter.DynamicField, "Rank", "U"u8);
                entry.EndWriting();
            }

            writer.Commit();
        }

        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var entry = writer.Update("users/1"u8))
            {
                entry.Write(0, "users/1"u8);
                entry.Write(1, "Oren"u8);
                entry.Write(Constants.IndexWriter.DynamicField,"Name", "Oren"u8);
                entry.EndWriting();
            }
            writer.Commit();
        }


        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var entry = writer.Update("users/1"u8))
            {
                entry.Write(0, "users/1"u8);
                entry.Write(1, "Eini"u8);
                entry.Write(Constants.IndexWriter.DynamicField,"Name", "Eini"u8);
                entry.EndWriting();
            }
           
            writer.Commit();
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
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using IndexFieldsMapping fields = PrepareSpatial(bsc);
        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var builder = writer.Index(IdString))
            {
                builder.Write(0, Encodings.Utf8.GetBytes(IdString));
                var spatialEntry = new CoraxSpatialPointEntry(latitude, longitude, geohash);
                builder.WriteSpatial(Constants.IndexWriter.DynamicField,"Coordinates_Home", spatialEntry);
                builder.EndWriting();
            }
            writer.Commit();
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

        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            writer.TryDeleteEntry(IdString);
            writer.Commit();
        }

        using (var searcher = new IndexSearcher(Env, fields))
        {
            Span<long> ids = new long[16];
            var entries = searcher.TermQuery("Coordinates_Home", geohash);
            Assert.Equal(0, entries.Fill(ids));
        }
    }

    const string IdString = "entry-1";

    private IndexFieldsMapping PrepareSpatial(ByteStringContext ctx)
    {
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
    public void WriteAndReadSpatialListDynamically(int size, double[] lat, double[] lon)
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using IndexFieldsMapping fields = PrepareSpatial(bsc);
        
        long entryId;
        using (var indexWriter = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var writer = indexWriter.Index("items/1"))
            {
                writer.Write(0, Encodings.Utf8.GetBytes("item/1"));
                writer.IncrementList();
                {
                    for (int i = 0; i < size; ++i)
                    {
                        var p = new CoraxSpatialPointEntry(lat[i], lon[i], Spatial4n.Util.GeohashUtils.EncodeLatLon(lat[i], lon[i], 9));
                        writer.WriteSpatial(Constants.IndexWriter.DynamicField, "CoordinatesIndex", p);
                    }
                }
                writer.DecrementList();
                entryId = writer.EntryId;
                writer.EndWriting();
            }
            indexWriter.Commit();
        }

        using (var indexSearcher = new IndexSearcher(Env, fields))
        {
            Page p = default;
            var reader = indexSearcher.GetEntryTermsReader(entryId, ref p);
            long fieldRootPage = indexSearcher.FieldCache.GetLookupRootPage("CoordinatesIndex");
            long i = 0;
            var l = new List<(double lat, double lng)>();
            for (int index = 0; index < size; index++)
            {
                l.Add((lat[index], lon[index]));
            }
            while (reader.FindNextSpatial(fieldRootPage))
            {
                i++;
                l.Remove((reader.Latitude, reader.Longitude));
            }
            Assert.Equal(size, i);
            Assert.Empty(l);
        }
        
    }
    
     
    [Fact]
    public void MixingStaticAndDynamicFieldsCorax3()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        Slice.From(bsc, "Id", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(bsc, "Name", ByteStringType.Immutable, out Slice dSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice);
        var fields = builder.Build();

        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var entryBuilder = writer.Index("users/1"))
            {
                entryBuilder.Write(0, "users/1"u8);
                entryBuilder.Write(1, "Oren"u8);
                entryBuilder.Write(Constants.IndexWriter.DynamicField,"Rank", "U"u8);
                entryBuilder.EndWriting();
            }
            writer.Commit();
        }
        
        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var entryBuilder = writer.Update("users/1"u8))
            {
                entryBuilder.Write(0, "users/1"u8);
                entryBuilder.Write(1, "Oren"u8);
                entryBuilder.Write(Constants.IndexWriter.DynamicField,"Name", "Maciej"u8);
                entryBuilder.EndWriting();
            }
          
            writer.Commit();
        }

        using (var searcher = new IndexSearcher(Env, fields))
        {
            Span<long> ids = stackalloc long[16];
            var read = searcher.TermQuery("Name", "Maciej").Fill(ids);
            Assert.Equal(1, read);
        }
        
        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            writer.TryDeleteEntry("users/1");
            writer.Commit();
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
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        Slice.From(bsc, "Id", ByteStringType.Immutable, out Slice aSlice);
        Slice.From(bsc, "Name", ByteStringType.Immutable, out Slice dSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, aSlice)
            .AddBinding(1, dSlice);
        var fields = builder.Build();

        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var entryBuilder = writer.Update("users/1"u8))
            {
                entryBuilder.Write(0, "users/1"u8);
                entryBuilder.Write(1, "Oren"u8);
                entryBuilder.Write(Constants.IndexWriter.DynamicField,"Rank", "U"u8);
                entryBuilder.EndWriting();
            }
            writer.Commit();
        }
        
        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var entryBuilder = writer.Update("users/1"u8))
            {
                entryBuilder.Write(0, "users/1"u8);
                entryBuilder.Write(1, "Oren"u8);
                entryBuilder.Write(Constants.IndexWriter.DynamicField,"Name", "Maciej"u8);
                entryBuilder.EndWriting();
            }
            writer.Commit();
        }

        using (var searcher = new IndexSearcher(Env, fields))
        {
            Span<long> ids = stackalloc long[16];
            var read = searcher.TermQuery("Name", "Maciej").Fill(ids);
            Assert.Equal(1, read);
        }
     
        using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var entry = writer.Update("users/1"u8))
            {
                entry.Write(0, "users/1"u8);
                entry.Write(1, "Eini"u8);
                entry.Write(Constants.IndexWriter.DynamicField,"Name", "Eini"u8);
                entry.EndWriting();
            }
            writer.Commit();
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
