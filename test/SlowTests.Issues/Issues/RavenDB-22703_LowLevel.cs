using System;
using System.Text;
using Corax;
using Corax.Analyzers;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using FastTests.Voron;
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
        const string compareWith = "compareWith";
        NonExisting_WhenIterateAndCompare_ShouldNotUseTheInvalidReader(WriteValue, CreateMultiUnaryItem);
        return;
        void WriteValue(IndexWriter.IndexEntryBuilder builder) => builder.Write(BarBoolIndex, Encoding.UTF8.GetBytes(compareWith));
        MultiUnaryItem CreateMultiUnaryItem(IndexSearcher searcher, FieldMetadata contentMetadata)
        {
            return new MultiUnaryItem(searcher, contentMetadata, "somevalue", UnaryMatchOperation.Equals);
        }
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public void NonExistingDouble_WhenIterateAndCompare_ShouldNotUseTheInvalidReader()
    {
        NonExisting_WhenIterateAndCompare_ShouldNotUseTheInvalidReader(WriteValue, CreateMultiUnaryItem);
        return;
        void WriteValue(IndexWriter.IndexEntryBuilder builder)
        {
            const long value = 8L;
            builder.Write(BarBoolIndex, null, value.ToString(), value, value);
        }
        
        MultiUnaryItem CreateMultiUnaryItem(IndexSearcher searcher, FieldMetadata contentMetadata)
        {
            return new MultiUnaryItem(contentMetadata, 0.0, UnaryMatchOperation.Equals);
        }
    }
    
    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public void NonExistingLong_WhenIterateAndCompare_ShouldNotUseTheInvalidReader()
    {
        NonExisting_WhenIterateAndCompare_ShouldNotUseTheInvalidReader(WriteValue, CreateMultiUnaryItem);
        return;
        void WriteValue(IndexWriter.IndexEntryBuilder builder)
        {
            builder.Write(BarBoolIndex, null, "8", 8, 8);
        }
        MultiUnaryItem CreateMultiUnaryItem(IndexSearcher searcher, FieldMetadata contentMetadata)
        {
            return new MultiUnaryItem(contentMetadata, 0L, UnaryMatchOperation.Equals);
        }
    }

    private void NonExisting_WhenIterateAndCompare_ShouldNotUseTheInvalidReader(Action<IndexWriter.IndexEntryBuilder> writeValue, Func<IndexSearcher, FieldMetadata, MultiUnaryItem> create)
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

            using (var indexSearcher = new IndexSearcher(Env, knownFields))
            {
                Span<long> ids = stackalloc long[10];
                var contentMetadata = indexSearcher.FieldMetadataBuilder("BarBool", BarBoolIndex);
                var match0 = indexSearcher.AllEntries();
                var match1 = create(indexSearcher, contentMetadata);

                var multiUnaryMatch = indexSearcher.CreateMultiUnaryMatch(match0, [match1]);

                Assert.Equal(0, multiUnaryMatch.Fill(ids)); //Thrown here
            }
        }
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
