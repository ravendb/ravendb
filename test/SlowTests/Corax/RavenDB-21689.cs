using System;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches.Meta;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_21689 : StorageTest
{
    private readonly ByteStringContext _bsc;
    private readonly IndexFieldsMapping _fieldsMapping;

    public RavenDB_21689(ITestOutputHelper output) : base(output)
    {
        _bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        _fieldsMapping = IndexFieldsMappingBuilder
            .CreateForWriter(false)
            .AddBinding(0, "id")
            .AddBinding(1, "boolean")
            .AddBinding(2, "text", LuceneAnalyzerAdapter.Create(new RavenStandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30), forQuerying: false))
            .Build();
    }
    
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void NotAcceleratedAndWithInsideSetTermMatchWillNotHaveInfinityLoop()
    {
        const int docsSize = 64 * 1000;
        
        using (var indexWriter = new IndexWriter(Env, _fieldsMapping, SupportedFeatures.All))
        {
            for (var docIdx = 0; docIdx < docsSize; ++docIdx)
            {
                using var builder = indexWriter.Index($"doc/{docIdx}");
                builder.Write(0, Encodings.Utf8.GetBytes($"doc/{docIdx}"));
                builder.Write(1, Encodings.Utf8.GetBytes("false"));
                builder.Write(2, Encodings.Utf8.GetBytes($"abc{docIdx}"));
                
            }

            indexWriter.Commit();
        }
        
        using (var indexSearcher = new IndexSearcher(Env, _fieldsMapping){ForceNonAccelerated = true})
        {
            var searchQuery = indexSearcher.SearchQuery(_fieldsMapping.GetByFieldId(2).Metadata, new[] {"abc10*"}, Constants.Search.Operator.Or);
            var termQuery = indexSearcher.TermQuery(_fieldsMapping.GetByFieldId(1).Metadata, "false");
            
            var searchIds = new long[docsSize];
            Span<long> fillBuffer = searchIds;
            var results = 0;
            while (searchQuery.Fill(fillBuffer) is var read and > 0)
            {
                fillBuffer = fillBuffer.Slice(read);
                results += read;
            }
            searchIds = searchIds[..results];
            searchIds.AsSpan().Sort();

            var termQueries = new long[docsSize];
            fillBuffer = termQueries;
            results = 0;
            while (termQuery.Fill(fillBuffer) is var read and > 0)
            {
                fillBuffer = fillBuffer.Slice(read);
                results += read;
            }

            termQueries = termQueries[..results];
            termQuery = indexSearcher.TermQuery(_fieldsMapping.GetByFieldId(1).Metadata, "false");

            var maxResults = Math.Max(searchIds.Length, termQueries.Length);
            var andBuffer = new long[maxResults];
            searchIds.CopyTo(andBuffer, 0);
            var mergeCount = MergeHelper.And(termQueries.AsSpan(), termQueries.AsSpan(), searchIds.AsSpan());
            Assert.Equal(mergeCount, termQuery.AndWith(andBuffer, searchIds.Length));
            Assert.True(termQueries.AsSpan(0, mergeCount).SequenceEqual(andBuffer.AsSpan(0, mergeCount)));
        }
    }


    public override void Dispose()
    {
        _bsc?.Dispose();
        _fieldsMapping?.Dispose();
        base.Dispose();
    }
}
