using System.Collections.Generic;
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
    // page: the same total, the same documents in the same order, the same scores. The main index here has many
    // segments, some with deletions and some without, so every branch runs: segments counted from the statistics,
    // segments walked because they have deletions, and the segment where the walk stops as soon as the page is full.
    public class StatisticsBackedNonSortingCollectorTests : NoDisposalNeeded
    {
        // built once for the class, the cases only read it
        private static readonly RAMDirectory MultiSegmentIndex = BuildIndex(segmentSize: 100, numberOfDocuments: 41_000,
            deleteIds: Enumerable.Range(150, 30).Concat(Enumerable.Range(400, 100)).Concat(new[] { 999 }));

        private static readonly Query[] Queries =
        {
            new MatchAllDocsQuery(),
            new TermQuery(new Term("Status", "active")),
            new TermQuery(new Term("Status", "expired")),
            new TermQuery(new Term("Status", "no-such-status")),
            new TermQuery(new Term("Id", "42")),
        };

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
            using (var reader = IndexReader.Open(MultiSegmentIndex, true, null))
            using (var searcher = new IndexSearcher(reader))
            {
                var segments = reader.GetSequentialSubReaders();
                Assert.True(segments.Length >= 5, "the index should have several segments");
                Assert.Contains(segments, s => s.HasDeletions);
                Assert.Contains(segments, s => s.HasDeletions == false);

                foreach (var query in Queries)
                    AssertSameAsWalkingCollector(searcher, query, numberOfDocsToCollect);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(1000)]
        public void ReturnsWhatTheWalkingCollectorReturns_OnOneSegmentAndOnAnEmptyIndex(int numberOfDocsToCollect)
        {
            // one segment without deletions, so the page fills in the only segment and the early stop ends the whole walk
            using (var directory = BuildIndex(segmentSize: 100_000, numberOfDocuments: 500, deleteIds: Enumerable.Empty<int>()))
            using (var reader = IndexReader.Open(directory, true, null))
            using (var searcher = new IndexSearcher(reader))
            {
                Assert.Single(reader.GetSequentialSubReaders());
                Assert.False(reader.HasDeletions);

                foreach (var query in Queries)
                    AssertSameAsWalkingCollector(searcher, query, numberOfDocsToCollect);
            }

            // one segment with deletions, so a term query walks all of it
            using (var directory = BuildIndex(segmentSize: 100_000, numberOfDocuments: 500, deleteIds: Enumerable.Range(0, 100)))
            using (var reader = IndexReader.Open(directory, true, null))
            using (var searcher = new IndexSearcher(reader))
            {
                Assert.Single(reader.GetSequentialSubReaders());
                Assert.True(reader.HasDeletions);

                foreach (var query in Queries)
                    AssertSameAsWalkingCollector(searcher, query, numberOfDocsToCollect);
            }

            using (var directory = BuildIndex(segmentSize: 100_000, numberOfDocuments: 0, deleteIds: Enumerable.Empty<int>()))
            using (var reader = IndexReader.Open(directory, true, null))
            using (var searcher = new IndexSearcher(reader))
            {
                Assert.Equal(0, reader.MaxDoc);

                foreach (var query in Queries)
                    AssertSameAsWalkingCollector(searcher, query, numberOfDocsToCollect);
            }
        }

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        public void LeavesOtherQueriesToTheCaller()
        {
            using (var reader = IndexReader.Open(MultiSegmentIndex, true, null))
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

        private static void AssertSameAsWalkingCollector(IndexSearcher searcher, Query query, int numberOfDocsToCollect)
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

        private static RAMDirectory BuildIndex(int segmentSize, int numberOfDocuments, IEnumerable<int> deleteIds)
        {
            var directory = new RAMDirectory();

            using (var writer = new IndexWriter(directory, new KeywordAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED, null))
            {
                // a segment per segmentSize documents and no merging, so deletions stay in the segments that received them
                writer.SetMaxBufferedDocs(segmentSize);
                writer.MergeFactor = 1000;

                for (var i = 0; i < numberOfDocuments; i++)
                {
                    // the multi-segment index ends with one large segment, with enough hits past any page for the
                    // early stop to be worth taking
                    if (i == 1000)
                        writer.SetMaxBufferedDocs(100_000);

                    writer.AddDocument(CreateDocument(i), null);
                }

                foreach (var id in deleteIds)
                    writer.DeleteDocuments(new Term("Id", id.ToString()), null);

                writer.Commit(null);
            }

            return directory;
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
