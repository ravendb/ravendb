using System;
using System.Collections.Generic;
using Corax;
using Corax.Mappings;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace SlowTests.Corax.Bugs;

public class RavenDB_27171(ITestOutputHelper output) : StorageTest(output)
{
    private const int IdFieldId = 0, DateFieldId = 1;
    
    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(false)]
    [InlineData(true)]
    public void MixedTextualAndNumericWritesOfSameTermAreRemovedCorrectly(bool sharedWithAnotherDocument)
    {
        using (var bsc = new ByteStringContext(SharedMultipleUseFlag.None))
        using (var knownFields = CreateKnownFields(bsc))
        {
            using (var indexWriter = new IndexWriter(Env, knownFields, SupportedFeatures.All))
            {
                using (var builder = indexWriter.Index("items/1"))
                {
                    builder.Write(IdFieldId, "items/1"u8);
                    builder.Write(DateFieldId, "5"u8);
                    builder.Write(DateFieldId, "5"u8, 5L, 5.0);
                    builder.EndWriting();
                }

                if (sharedWithAnotherDocument)
                {
                    using (var builder = indexWriter.Index("items/2"))
                    {
                        builder.Write(IdFieldId, "items/2"u8);
                        builder.Write(DateFieldId, "5"u8, 5L, 5.0);
                        builder.EndWriting();
                    }
                }

                indexWriter.Commit();
            }

            using (var indexWriter = new IndexWriter(Env, knownFields, SupportedFeatures.All))
            {
                Assert.True(indexWriter.TryDeleteEntry("items/1"));
                indexWriter.Commit();
            }
        }

        using (var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator)))
        {
            var expected = sharedWithAnotherDocument ? 1 : 0;

            Assert.Equal(expected, CountDistinctEntries(
                searcher.BetweenQuery(searcher.FieldMetadataBuilder("Date", DateFieldId), long.MinValue, long.MaxValue)));

            Assert.Equal(expected, CountDistinctEntries(
                searcher.BetweenQuery(searcher.FieldMetadataBuilder("Date", DateFieldId), double.MinValue, double.MaxValue)));
        }
    }
    
    [RavenTheory(RavenTestCategory.Corax)]
    [InlineData(true)]
    [InlineData(false)]
    public void DuplicatedNumericValuesAreRemovedCorrectly(bool numericalValuesWithoutFrequencies)
    {
        var supportedFeatures = numericalValuesWithoutFrequencies
            ? SupportedFeatures.All
            : new SupportedFeatures(isPhraseQuerySupported: true, isStoreOnlySupported: true, isPaginationBasedOnEntryIdSupported: true, isNumericalValuesWithoutFrequenciesSupported: false);

        using (var bsc = new ByteStringContext(SharedMultipleUseFlag.None))
        using (var knownFields = CreateKnownFields(bsc))
        {
            using (var indexWriter = new IndexWriter(Env, knownFields, supportedFeatures))
            {
                using (var builder = indexWriter.Index("items/1"))
                {
                    builder.Write(IdFieldId, "items/1"u8);
                    for (int i = 0; i < 3; i++)
                        builder.Write(DateFieldId, "5"u8, 5L, 5.0);
                    builder.EndWriting();
                }
                
                using (var builder = indexWriter.Index("items/2"))
                {
                    builder.Write(IdFieldId, "items/2"u8);
                    builder.Write(DateFieldId, "5"u8, 5L, 5.0);
                    builder.EndWriting();
                }

                indexWriter.Commit();
            }

            using (var indexWriter = new IndexWriter(Env, knownFields, supportedFeatures))
            {
                Assert.True(indexWriter.TryDeleteEntry("items/1"));
                Assert.True(indexWriter.TryDeleteEntry("items/2"));
                indexWriter.Commit();
            }
        }

        using (var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator)))
        {
            Assert.Equal(0, CountDistinctEntries(
                searcher.BetweenQuery(searcher.FieldMetadataBuilder("Date", DateFieldId), long.MinValue, long.MaxValue)));

            Assert.Equal(0, CountDistinctEntries(
                searcher.BetweenQuery(searcher.FieldMetadataBuilder("Date", DateFieldId), double.MinValue, double.MaxValue)));
        }
    }

    private static int CountDistinctEntries(global::Corax.Querying.Matches.MultiTermMatch match)
    {
        var ids = new HashSet<long>();
        Span<long> buffer = new long[256];
        int read;
        while ((read = match.Fill(buffer)) != 0)
        {
            for (int i = 0; i < read; i++)
                ids.Add(buffer[i]);
        }

        return ids.Count;
    }

    private IndexFieldsMapping CreateKnownFields(ByteStringContext bsc)
    {
        Slice.From(bsc, "Id", ByteStringType.Immutable, out Slice idSlice);
        Slice.From(bsc, "Date", ByteStringType.Immutable, out Slice dateSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(IdFieldId, idSlice)
            .AddBinding(DateFieldId, dateSlice);
        return builder.Build();
    }
}
