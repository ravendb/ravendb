using System;
using Corax;
using Corax.Querying;
using Corax.Mappings;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Sparrow.Server;
using Voron;
using Xunit;
using Xunit.Abstractions;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

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
        using (var indexWriter = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var builder = indexWriter.Index("users/1"u8))
            {
                builder.Write(0, null, "users/1"u8);
                builder.Write(1, null, "Oren Eini"u8);
            }
            indexWriter.Commit();
        }
        
        {
            using var indexSearcher = new IndexSearcher(Env, fields);
            var match = indexSearcher.TermQuery("Content", "eini");
            Assert.Equal(1, match.Fill(ids));
            Assert.Equal("users/1", indexSearcher.TermsReaderFor("Id").GetTermFor(ids[0]));
        }
    }
    
      
    [Fact]
    public void CanDetectWhenFieldHasMultipleTerms()
    {
        var fields = CreateKnownFields(Allocator);
        Span<long> ids = stackalloc long[1024];
        using (var indexWriter = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var builder = indexWriter.Index("users/1"u8))
            {
                builder.Write(0, null, "users/1"u8);
                builder.Write(1, null, "Oren Eini"u8);
                builder.IncrementList();
                builder.Write(Constants.IndexWriter.DynamicField, "Items","One");
                builder.Write(Constants.IndexWriter.DynamicField, "Items","Two");
                builder.DecrementList();
            }
            indexWriter.Commit();
        }
        
        {
            using var indexSearcher = new IndexSearcher(Env, fields);
          
            Assert.True(indexSearcher.HasMultipleTermsInField("Content"));
            Assert.True(indexSearcher.HasMultipleTermsInField("Items"));
            Assert.False(indexSearcher.HasMultipleTermsInField("id()"));
        }
    }
    
     
    [Fact]
    public void CanUpdateWithDifferentFrequency()
    {
        var fields = CreateKnownFields(Allocator);
        Span<long> ids = stackalloc long[1024];
        using (var indexWriter = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var builder = indexWriter.Index("users/1"u8))
            {
                builder.Write(0, null, "users/1"u8);
                builder.Write(1, null, "Oren Oren Oren Rahien"u8);
            }
            indexWriter.Commit();
        }
        
        using (var indexWriter = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var builder = indexWriter.Update("users/1"u8))
            {
                builder.Write(0, null, "users/1"u8);
                builder.Write(1, null, "Oren Eini"u8);
            }
            using (var builder = indexWriter.Update("users/2"u8))
            {
                builder.Write(0, null, "users/2"u8);
                builder.Write(1, null, "Oren Oren"u8);
            }
            indexWriter.Commit();
        }
        
        {
            using var indexSearcher = new IndexSearcher(Env, fields);
            
            var match = indexSearcher.TermQuery("Content", "oren", hasBoost: true);
            var sort = indexSearcher.OrderBy(match, new OrderMetadata(true, MatchCompareFieldType.Score));
            Assert.Equal(2, sort.Fill(ids));
            Assert.Equal("users/2", indexSearcher.TermsReaderFor("Id").GetTermFor(ids[0]));
            Assert.Equal("users/1", indexSearcher.TermsReaderFor("Id").GetTermFor(ids[1]));
            
            match = indexSearcher.TermQuery("Content", "rahien");
            Assert.Equal(0, match.Fill(ids));
        }
    }

    
    [Fact]
    public void CanUpdateUsingBuilder()
    {
        var fields = CreateKnownFields(Allocator);
        Span<long> ids = stackalloc long[1024];
        using (var indexWriter = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var builder = indexWriter.Index("users/1"u8))
            {
                builder.Write(0, null, "users/1"u8);
                builder.Write(1, null, "Oren Eini"u8);
            }
            indexWriter.Commit();
        }
        
        using (var indexWriter = new IndexWriter(Env, fields, SupportedFeatures.All))
        {
            using (var builder = indexWriter.Update("users/1"u8))
            {
                builder.Write(0, null, "users/1"u8);
                builder.Write(1, null, "Ayende Rahien"u8);
            }
            indexWriter.Commit();
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
