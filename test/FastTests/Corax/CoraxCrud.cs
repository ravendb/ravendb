using System;
using Corax;
using Corax.Mappings;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Sparrow.Server;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class CoraxCrud: StorageTest
{
    public CoraxCrud(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CanIndexUsingBuilder()
    {
        var fields = CreateKnownFields(Allocator);
        Span<long> ids = stackalloc long[1024];
        using (var indexWriter = new IndexWriter(Env, fields))
        {
            using (var builder = indexWriter.Index("users/1"u8))
            {
                builder.Write(0, null, "users/1"u8);
                builder.Write(1, null, "Oren Eini"u8);
            }
            indexWriter.PrepareAndCommit();
        }
        
        {
            using var indexSearcher = new IndexSearcher(Env, fields);
            var match = indexSearcher.TermQuery("Content", "eini");
            Assert.Equal(1, match.Fill(ids));
            Assert.Equal("users/1", indexSearcher.TermsReaderFor("Id").GetTermFor(ids[0]));
        }
    }
    
    [Fact]
    public void CanUpdateUsingBuilder()
    {
        var fields = CreateKnownFields(Allocator);
        Span<long> ids = stackalloc long[1024];
        using (var indexWriter = new IndexWriter(Env, fields))
        {
            using (var builder = indexWriter.Index("users/1"u8))
            {
                builder.Write(0, null, "users/1"u8);
                builder.Write(1, null, "Oren Eini"u8);
            }
            indexWriter.PrepareAndCommit();
        }
        
        using (var indexWriter = new IndexWriter(Env, fields))
        {
            using (var builder = indexWriter.Update("users/1"u8))
            {
                builder.Write(0, null, "users/1"u8);
                builder.Write(1, null, "Ayende Rahien"u8);
            }
            indexWriter.PrepareAndCommit();
        }
        
        {
            using var indexSearcher = new IndexSearcher(Env, fields);
            
            var match = indexSearcher.TermQuery("Content", "ayende");
            Assert.Equal(1, match.Fill(ids));
            Assert.Equal("users/1", indexSearcher.TermsReaderFor("Id").GetTermFor(ids[0]));
            
            match = indexSearcher.TermQuery("Content", "eini");
            Assert.Equal(0, match.Fill(ids));
        }
    }
    
    private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx)
    {
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
        Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

        using (var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
                   .AddBinding(0, idSlice)
                   .AddBinding(1, contentSlice, LuceneAnalyzerAdapter.Create(new RavenStandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30))))
            return builder.Build();
    }

}
