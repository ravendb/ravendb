using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;
using Corax;
using Corax.Analyzers;
using Corax.Mappings;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Raven.Server.Documents.Queries;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
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

    [RavenFact(RavenTestCategory.Corax)]
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

    [RavenTheory(RavenTestCategory.Corax)]
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

    [RavenFact(RavenTestCategory.Corax)]
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

        // 3 matches: entries 0,1 (maciej) and 3 (kaszebe).
        // kaszebe appears once vs maciej twice → higher BM25 IDF → entry 3 scores highest.
        var results = ExecuteRQLQueryByScore(
            "FROM RankingIndex WHERE Content = 'maciej' OR Content = 'kaszebe' ORDER BY score()");

        Assert.Equal(3, results.Count);
        Assert.Equal("3", results[0]); // kaszebe (highest IDF) scores first
    }
    
    [RavenFact(RavenTestCategory.Corax)]
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

        // AND of two terms: entries 0 (maciej+kaszebe) and 3 (kaszebe+maciej) match both.
        var results = ExecuteRQLQueryByScore(
            "FROM RankingIndex WHERE Content = 'maciej' AND Content = 'kaszebe' ORDER BY score()");

        Assert.Equal(2, results.Count);
    }


    [RavenFact(RavenTestCategory.Corax)]
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
    
    private List<string> ExecuteRQLQueryByScore(string rqlQuery)
    {
        using var searcher = new IndexSearcher(Env, _mapping);
        var queryMetadata = new QueryMetadata(rqlQuery, null, 0);
        var planParams = new PlanParameters
        {
            IndexSearcher = searcher,
            Metadata = queryMetadata,
            HasBoost = true,
            Allocator = Allocator
        };
        var match = QueryPlanBuilder.BuildFilterMatch(planParams, new QueryBuilderParameters(searcher, Allocator, queryMetadata, null, _mapping, hasBoost: true), null, false, default);
        match = ApplyScoreOrderingIfRequested(searcher, queryMetadata, match, long.MaxValue);
        var list = new List<string>();
        Span<long> ids = stackalloc long[256];
        int count;
        while ((count = match.Fill(ids)) > 0)
            for (int i = 0; i < count; i++)
                list.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(ids[i]));
        return list;
    }

    // Wraps the searcher's score-ordering primitive directly. Production OrderBy(QueryBuilderParameters, ...)
    // needs the full server-side query pipeline that these direct-IndexSearcher tests bypass.
    private static global::Corax.Querying.Matches.Meta.IQueryMatch ApplyScoreOrderingIfRequested(IndexSearcher searcher, QueryMetadata queryMetadata, global::Corax.Querying.Matches.Meta.IQueryMatch match, long take)
    {
        var orderByFields = queryMetadata.OrderBy;
        if (orderByFields is null || orderByFields.Length == 0)
            return match;

        int takeInt = take > int.MaxValue ? global::Corax.Constants.IndexSearcher.TakeAll : (int)take;
        foreach (var field in orderByFields)
        {
            if (field.OrderingType == Raven.Server.Documents.Queries.AST.OrderByFieldType.Score)
            {
                var meta = new global::Corax.Utils.OrderMetadata(true, global::Corax.Querying.Matches.SortingMatches.Meta.MatchCompareFieldType.Score, field.Ascending);
                return searcher.OrderBy(match, meta, global::Corax.Utils.NullsSortMode.NullsLargest, take: takeInt);
            }
        }

        return match;
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

    public override async ValueTask DisposeAsync()
    {
        _context.Dispose();
        _mapping.Dispose();
        await base.DisposeAsync();
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
