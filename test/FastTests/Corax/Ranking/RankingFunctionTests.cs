using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Corax;
using Corax.Analyzers;
using Corax.Querying;
using Corax.Mappings;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Xunit;
using Xunit.Abstractions;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace FastTests.Corax.Ranking;

public class RankingFunctionTests : StorageTest
{
    private readonly ByteStringContext _context;
    private readonly IndexFieldsMapping _mapping;
    private const int IdIndex = 0, ContentIndex = 1;

    public RankingFunctionTests(ITestOutputHelper output) : base(output)
    {
        //Lets use FullTextSearch analyzer for Content. This allows us to produce multiple items from one input string but
        //be careful what are you querying :) 
        Analyzer fullTextSearch = LuceneAnalyzerAdapter.Create(new RavenStandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30), forQuerying: false);
        _context = new ByteStringContext(SharedMultipleUseFlag.None);
        _mapping = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(IdIndex, "Id")
            .AddBinding(ContentIndex, "Content", fullTextSearch)
            .Build();
    }

    [Fact]
    public void CanGenerateRankingForSingleInTermMatch()
    {
        // we've to provide at least two docs into index. If not IDF will be 0. Consequence of that is score equal to 0.
        IndexEntries(new List<EntryData>() {new(1, "maciej maciej"), new(2, "jan"), new(3, "Remus")});
        using var indexSearcher = new IndexSearcher(Env, _mapping);

        var query = indexSearcher.TermQuery(_mapping.GetByFieldId(1).Metadata.ChangeScoringMode(true), "maciej");
        Span<float> scores = stackalloc float[2];
        Span<long> ids = stackalloc long[2];

        var read = query.Fill(ids);
        query.Score(ids.Slice(0, read), scores, 0);

        Assert.Equal(1, read);
    }

    [Theory]
    [InlineData(10)] //small
    [InlineData(1000)] //posting list

    public void CanGenerateRankingForContainers(int size)
    {
        // we've to provide at least two docs into index. If not IDF will be 0. Consequence of that is score equal to 0.
        var list = new List<EntryData>();
        var sb = new StringBuilder();
        for (int i = 0; i < size; ++i)
        {
            sb.Append(" Maciej");
            list.Add(new(i, sb.ToString()));
        }

        IndexEntries(list);

        using var indexSearcher = new IndexSearcher(Env, _mapping);

        var query = indexSearcher.TermQuery(_mapping.GetByFieldId(1).Metadata.ChangeScoringMode(true), "maciej");
        Span<float> scores = new float[size];
        Span<long> ids = new long[size];

        var read = query.Fill(ids);
        query.Score(ids.Slice(0, read), scores, 0);
    }

    [Fact]
    public void TwoBoostingMatchesWithOr()
    {
        var list = new List<EntryData>();
        {
            var idX = 0;
            list.Add(new EntryData(idX++, "Maciej"));
            list.Add(new EntryData(idX++, "Maciej"));
            list.Add(new EntryData(idX++, "Jan"));
            list.Add(new EntryData(idX++, "Kaszebe"));
        }
        
        IndexEntries(list);
        using var indexSearcher = new IndexSearcher(Env, _mapping);
        var q1 = indexSearcher.TermQuery(_mapping.GetByFieldId(1).Metadata.ChangeScoringMode(true), "maciej");
        var q2 = indexSearcher.TermQuery(_mapping.GetByFieldId(1).Metadata.ChangeScoringMode(true), "kaszebe");

        var orMatch = indexSearcher.Or(q1, q2);
        Span<float> scores = stackalloc float[10];
        scores.Fill(0);
        Span<long> ids = stackalloc long[10];
        
        var read = orMatch.Fill(ids);
        orMatch.Score(ids.Slice(0, read), scores.Slice(0, read), 0);
        
        Assert.Equal(3, read);
        
        MemoryExtensions.Sort(ids.Slice(0, read), scores.Slice(0, read));
        long id = ids[2];
        Assert.Equal("3", indexSearcher.TermsReaderFor(indexSearcher.GetFirstIndexedFiledName()).GetTermFor(id));
    }
    
    [Fact]
    public void TwoBoostingMatchesWithAnd()
    {
        var list = new List<EntryData>();
        {
            var idX = 0;
            list.Add(new EntryData(idX++, "Maciej Kaszebe Kaszebe Kaszebe Kaszebe"));
            list.Add(new EntryData(idX++, "Maciej"));
            list.Add(new EntryData(idX++, "Jan"));
            list.Add(new EntryData(idX++, "Kaszebe Maciej Maciej Maciej"));
        }
        
        IndexEntries(list);
        using var indexSearcher = new IndexSearcher(Env, _mapping);
        var q1 = indexSearcher.TermQuery(_mapping.GetByFieldId(1).Metadata.ChangeScoringMode(true), "maciej");
        var q2 = indexSearcher.TermQuery(_mapping.GetByFieldId(1).Metadata.ChangeScoringMode(true), "kaszebe");

        var andMatch = indexSearcher.And(q1, q2);
        Span<float> scores = stackalloc float[10];
        scores.Fill(0);
        Span<long> ids = stackalloc long[10];
        
        var read = andMatch.Fill(ids);
        andMatch.Score(ids.Slice(0, read), scores.Slice(0, read), 1f);
        
        Assert.Equal(2, read);
        
        MemoryExtensions.Sort(ids.Slice(0, read), scores.Slice(0, read));
        
       // Assert.Equal("id/3", indexSearcher.GetIdentityFor(ids[2]));
    }


    [Fact]
    public void MultiTermMatch()
    {
        var list = new List<EntryData>();
        {
            var idX = 0;
            list.Add(new EntryData(idX++, "Macedonia")); // id0
            list.Add(new EntryData(idX++, "Jan")); // id1
            list.Add(new EntryData(idX++, "Maciej")); //id2
            list.Add(new EntryData(idX++, "Maciek")); //id3
            list.Add(new EntryData(idX++, "Maciej Maciej Maciej")); //id4

        }
        
        IndexEntries(list);
        using var indexSearcher = new IndexSearcher(Env, _mapping);
        var query = indexSearcher.StartWithQuery(_mapping.GetByFieldId(1).Metadata.ChangeScoringMode(true), "mac");
        Span<long> matches = stackalloc long[10];
        Span<float> scores = stackalloc float[10];
        scores.Fill(0);
        var read = query.Fill(matches);
        query.Score(matches.Slice(0, read), scores.Slice(0, read), 0);
        Assert.Equal(4, read);
        
        scores.Slice(0,4).Sort(matches.Slice(0, 4));
        var ids = new List<string>();
        for (int i = 0; i < 4; ++i)
        {
            long id = matches[i];
            ids.Add(indexSearcher.TermsReaderFor(indexSearcher.GetFirstIndexedFiledName()).GetTermFor(id));
        }
    }
    
    private void IndexEntries(IEnumerable<EntryData> entries)
    {
        using var indexWriter = new IndexWriter(Env, _mapping, SupportedFeatures.All);
        
        foreach (var dto in entries)
        {
            using var builder = indexWriter.Index(dto.Id.ToString());
            builder.Write(IdIndex, dto.IdAsSpan, dto.Id, dto.Id);
            builder.Write(ContentIndex, dto.ContentAsSpan);
            builder.EndWriting();
        }

        indexWriter.Commit();
    }

    public override void Dispose()
    {
        _context.Dispose();
        _mapping.Dispose();
        base.Dispose();
    }

    private class EntryData
    {
        public EntryData(long id, string content)
        {
            Id = id;
            Content = content;
        }

        public long Id { get; set; }

        public Span<byte> IdAsSpan => Encodings.Utf8.GetBytes(Id.ToString(CultureInfo.InvariantCulture));

        public string Content { get; set; }

        public Span<byte> ContentAsSpan => Encodings.Utf8.GetBytes(Content);
    }
}
