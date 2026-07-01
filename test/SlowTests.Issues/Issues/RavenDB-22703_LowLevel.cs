using System;
using System.Collections.Generic;
using System.Text;
using Corax;
using Corax.Analyzers;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;
using Raven.Server.Documents.Queries;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_22703_LowLevel : StorageTest
{
    public RavenDB_22703_LowLevel(ITestOutputHelper output) : base(output)
    {
    }

    private const int IdIndex = 0, BarBoolIndex = 1;

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public void TestNonExistingPostingList()
    {
        using (var bsc = new ByteStringContext(SharedMultipleUseFlag.None))
        {
            var knownFields = CreateKnownFields(bsc);

            using (var indexWriter = new IndexWriter(Env, knownFields, SupportedFeatures.All))
            {
                using (var builder = indexWriter.Index("bars/1"))
                {
                    builder.Write(IdIndex, "bars/1"u8);
                    builder.Write(BarBoolIndex, "false"u8);
                    builder.EndWriting();
                }

                using (var builder = indexWriter.Index("bars/2"))
                {
                    builder.Write(IdIndex, "bars/2"u8);
                    builder.Write(BarBoolIndex, Constants.NullValueSpan);
                    builder.EndWriting();
                }

                using (var builder = indexWriter.Index("bars/3"))
                {
                    builder.Write(IdIndex, "bars/3"u8);
                    builder.Write(BarBoolIndex, Constants.NonExistingValueSlice);
                    builder.EndWriting();
                }

                indexWriter.Commit();
            }

            using (var indexSearcher = new IndexSearcher(Env, knownFields))
            {
                var barBoolField = FieldMetadata.Build(knownFields.GetByFieldId(BarBoolIndex).FieldName, default, BarBoolIndex, default, default);

                indexSearcher.TryGetPostingListForNull(barBoolField, out long nullPostingListId);
                indexSearcher.TryGetPostingListForNonExisting(barBoolField, out long nonExistingPostingListId);

                var nullPostingList = indexSearcher.GetPostingList(nullPostingListId);
                var nonExistingPostingList = indexSearcher.GetPostingList(nonExistingPostingListId);

                Assert.Equal(1, nullPostingList.State.LeafPages);
                Assert.Equal(1, nonExistingPostingList.State.LeafPages);
            }
        }
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public void NonExistingLiteral_WhenIterateAndCompare_ShouldNotUseTheInvalidReader()
    {
        // bars/1 has BarBool = "compareWith"; query searches for "somevalue" → 0 results expected.
        NonExisting_WhenIterateAndCompare_ShouldNotUseTheInvalidReader(
            builder => builder.Write(BarBoolIndex, Encoding.UTF8.GetBytes("compareWith")),
            "FROM TestIndex WHERE BarBool = 'somevalue'");
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public void NonExistingDouble_WhenIterateAndCompare_ShouldNotUseTheInvalidReader()
    {
        // bars/1 has BarBool = 8 (long/double); query searches for 0.0 → 0 results expected.
        NonExisting_WhenIterateAndCompare_ShouldNotUseTheInvalidReader(
            builder => { const long value = 8L; builder.Write(BarBoolIndex, null, value.ToString(), value, value); },
            "FROM TestIndex WHERE BarBool = 0.0");
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public void NonExistingLong_WhenIterateAndCompare_ShouldNotUseTheInvalidReader()
    {
        // bars/1 has BarBool = 8 (long); query searches for 0 → 0 results expected.
        NonExisting_WhenIterateAndCompare_ShouldNotUseTheInvalidReader(
            builder => builder.Write(BarBoolIndex, null, "8", 8, 8),
            "FROM TestIndex WHERE BarBool = 0");
    }

    private void NonExisting_WhenIterateAndCompare_ShouldNotUseTheInvalidReader(Action<IndexWriter.IndexEntryBuilder> writeValue, string rqlQuery)
    {
        using (var bsc = new ByteStringContext(SharedMultipleUseFlag.None))
        {
            var knownFields = CreateKnownFields(bsc);

            using (var indexWriter = new IndexWriter(Env, knownFields, SupportedFeatures.All))
            {
                using (var builder = indexWriter.Index("bars/1"))
                {
                    writeValue(builder);
                    builder.EndWriting();
                }

                using (var builder = indexWriter.Index("bars/2"))
                {
                    builder.Write(BarBoolIndex, Constants.NullValueSpan);
                    builder.EndWriting();
                }

                using (var builder = indexWriter.Index("bars/3"))
                {
                    builder.Write(BarBoolIndex, Constants.NonExistingValueSlice);
                    builder.EndWriting();
                }

                indexWriter.Commit();
            }

            // Querying for a non-existent value must return 0 results without crashing.
            var results = ExecuteRQLQuery(knownFields, rqlQuery);
            Assert.Equal(0, results.Count); //Should not throw here
        }
    }

    private List<string> ExecuteRQLQuery(IndexFieldsMapping knownFields, string rqlQuery)
    {
        using var searcher = new IndexSearcher(Env, knownFields);
        var queryMetadata = new QueryMetadata(rqlQuery, null, 0);
        var planParams = new PlanParameters
        {
            IndexSearcher = searcher,
            Metadata = queryMetadata,
            QueryParameters = null,
            Allocator = Allocator
        };
        var match = QueryPlanBuilder.BuildFilterMatch(planParams, new QueryBuilderParameters(searcher, Allocator, queryMetadata, null, knownFields), null, false, default);
        var list = new List<string>();
        Span<long> ids = stackalloc long[256];
        int count;
        while ((count = match.Fill(ids)) > 0)
            for (int i = 0; i < count; i++)
                list.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(ids[i]));
        return list;
    }

    private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx, Analyzer analyzer = null)
    {
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
        Slice.From(ctx, "BarBool", ByteStringType.Immutable, out Slice barBoolSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(IdIndex, idSlice, analyzer)
            .AddBinding(BarBoolIndex, barBoolSlice, analyzer);
        return builder.Build();
    }
}
