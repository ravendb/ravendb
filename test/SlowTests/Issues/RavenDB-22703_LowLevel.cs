using Corax;
using Corax.Analyzers;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22703_LowLevel : StorageTest
{
    public RavenDB_22703_LowLevel(ITestOutputHelper output) : base(output)
    {
    }
    
    private const int IdIndex = 0, BarBoolIndex = 1;
    
    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public void LowLevelTest()
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
