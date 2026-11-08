using System.Linq;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Collectors;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Lucene
{
    // The statistics-backed collector must return exactly what NonSortingCollector returns for the same query and
    // page: the same total, the same documents in the same order, the same scores. The index here has many segments,
    // some with deletions and some without, so every branch runs: segments counted from the statistics, segments
    // walked because they have deletions, and the segment where the walk stops as soon as the page is full.
    public class StatisticsBackedNonSortingCollectorTests : NoDisposalNeeded
    {
        public StatisticsBackedNonSortingCollectorTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(99)]
        [InlineData(100)]
        [InlineData(101)]
        [InlineData(250)]
        [InlineData(333)]
        [InlineData(999)]
        [InlineData(10_000)]
        [InlineData(100_000)]
        public void ReturnsWhatTheWalkingCollectorReturns(int numberOfDocsToCollect)
        {
            using (var directory = new RAMDirectory())
            {
                BuildIndex(directory);

                using (var reader = IndexReader.Open(directory, true, null))
                using (var searcher = new IndexSearcher(reader))
                {
                    var segments = reader.GetSequentialSubReaders();
                    Assert.True(segments.Length >= 5, "the index should have several segments");
                    Assert.Contains(segments, s => s.HasDeletions);
                    Assert.Contains(segments, s => s.HasDeletions == false);

                    var queries = new Query[]
                    {
                        new MatchAllDocsQuery(),
                        new TermQuery(new Term("Status", "active")),
                        new TermQuery(new Term("Status", "expired")),
                        new TermQuery(new Term("Status", "no-such-status")),
                        new TermQuery(new Term("Id", "42")),
                    };

                    foreach (var query in queries)
                    {
                        var walking = new NonSortingCollector(numberOfDocsToCollect);
                        searcher.Search(query, walking, null);

                        using (var expected = walking.ToTopDocs())
                        {
                            Assert.True(StatisticsBackedNonSortingCollector.TryExecute(searcher, query, numberOfDocsToCollect, null, out var actual));

                            using (actual)
                            {
                                Assert.Equal(expected.TotalHits, actual.TotalHits);
                                Assert.Equal(expected.MaxScore, actual.MaxScore);
                                Assert.Equal(expected.ScoreDocArray.Length, actual.ScoreDocArray.Length);

                                for (var i = 0; i < expected.ScoreDocArray.Length; i++)
                                    Assert.Equal(expected.ScoreDocArray[i], actual.ScoreDocArray[i]);
                            }
                        }
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        public void LeavesOtherQueriesToTheCaller()
        {
            using (var directory = new RAMDirectory())
            {
                BuildIndex(directory);

                using (var reader = IndexReader.Open(directory, true, null))
                using (var searcher = new IndexSearcher(reader))
                {
                    var boolean = new BooleanQuery
                    {
                        { new TermQuery(new Term("Status", "active")), Occur.MUST },
                        { new TermQuery(new Term("Id", "42")), Occur.MUST_NOT },
                    };

                    Assert.False(StatisticsBackedNonSortingCollector.TryExecute(searcher, boolean, 10, null, out var result));
                    Assert.Null(result);

                    Assert.False(StatisticsBackedNonSortingCollector.TryExecute(searcher, new PrefixQuery(new Term("Status", "act")), 10, null, out result));
                    Assert.Null(result);
                }
            }
        }

        private static void BuildIndex(Directory directory)
        {
            using (var writer = new IndexWriter(directory, new KeywordAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED, null))
            {
                // a segment per 100 documents and no merging, so deletions stay in the segments that received them
                writer.SetMaxBufferedDocs(100);
                writer.MergeFactor = 1000;

                for (var i = 0; i < 1000; i++)
                    writer.AddDocument(CreateDocument(i), null);

                // one large segment at the end, with enough hits past any page for the early stop to be worth taking
                writer.SetMaxBufferedDocs(100_000);
                for (var i = 1000; i < 41_000; i++)
                    writer.AddDocument(CreateDocument(i), null);

                // deletions in the second, fifth and last of the small segments, including one that empties a whole segment
                foreach (var id in Enumerable.Range(150, 30).Concat(Enumerable.Range(400, 100)).Concat(new[] { 999 }))
                    writer.DeleteDocuments(new Term("Id", id.ToString()), null);

                writer.Commit(null);
            }
        }

        private static Document CreateDocument(int i)
        {
            var document = new Document();
            document.Add(new Field("Id", i.ToString(), Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
            document.Add(new Field("Status", i % 4 == 0 ? "expired" : "active", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
            return document;
        }
    }
}
