using System;
using System.Linq;
using Corax;
using Corax.Querying;
using Corax.Mappings;
using Corax.Utils;
using Assert = Xunit.Assert;
using FastTests.Voron;
using Sparrow;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;
using Sparrow.Threading;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace FastTests.Corax;

public class SpatialTests : StorageTest
{
    private const int IdIndex = 0, CoordinatesIndex = 1;
    
    public SpatialTests(ITestOutputHelper output) : base(output)
    {
    }

    private static IndexFieldsMapping GetFieldsMapping(ByteStringContext ctx)
    {
        Slice.From(ctx, "Id", ByteStringType.Immutable, out var idSlice);
        Slice.From(ctx, "Coordinates", ByteStringType.Immutable, out var idCoordinates);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(IdIndex, idSlice)
            .AddBinding(CoordinatesIndex, idCoordinates);
        return builder.Build();
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(48.666708, -4.333999, "gbsuv7s04")]
    [InlineData(53.015261, 18.611487, "u3mjxe0kr")]
    public void CanIndexReadAndDeleteLongLatSpatial(double latitude, double longitude, string geohash)
    {
        var IdString = "entry-1";
        var geohashes = Enumerable.Range(1, geohash.Length)
            .Select(i => 
                geohash.Substring(0, i)
                )
            .ToList();

        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        var fieldsMapping = GetFieldsMapping(bsc);
        
        using (var writer = new IndexWriter(Env, fieldsMapping, SupportedFeatures.All))
        {
            using (var entry = writer.Index(IdString))
            {
                entry.Write(IdIndex, Encodings.Utf8.GetBytes(IdString));
                var spatialEntry = new CoraxSpatialPointEntry(latitude, longitude, geohash);
                entry.WriteSpatial(CoordinatesIndex, null, spatialEntry);
            }

            writer.Commit();
        }

        for (int i = 0; i < geohash.Length; ++i)
        {
            var partialGeohash = geohash.Substring(0, i + 1);
            using (var searcher = new IndexSearcher(Env, fieldsMapping))
            {
                Span<long> ids = new long[16];
                var entries = searcher.TermQuery("Coordinates", partialGeohash);
                Assert.Equal(1, entries.Fill(ids));
                Page p = default;
                var reader = searcher.GetEntryTermsReader(ids[0], ref p);
                long fieldRootPage = searcher.FieldCache.GetLookupRootPage(fieldsMapping.GetByFieldId(CoordinatesIndex).FieldName);
                Assert.True(reader.FindNextSpatial(fieldRootPage));
                
                Assert.Equal(reader.Latitude, latitude);
                Assert.Equal(reader.Longitude, longitude);
            }
        }

        using (var writer = new IndexWriter(Env, fieldsMapping, SupportedFeatures.All))
        {
            writer.TryDeleteEntry(IdString);
            writer.Commit();
        }

        using (var searcher = new IndexSearcher(Env, fieldsMapping))
        {
            Span<long> ids = new long[16];
            var entries = searcher.TermQuery("Coordinates", geohash);
            Assert.Equal(0, entries.Fill(ids));
        }
    }
}
