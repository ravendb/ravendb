using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using Corax.Mappings;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;
using Raven.Server.Documents.Queries;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace FastTests.Corax
{
    public class BoostingQueryTest : StorageTest
    {
        private List<IndexSingleNumericalEntry<long, long>> longList = new();
        private const int IndexId = 0, Content1 = 1, Content2 = 2;

        public BoostingQueryTest(ITestOutputHelper output) : base(output) { }


        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleBoosting()
        {
            PrepareData();
            IndexEntries();
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            {
                var match = searcher.AllEntries();
                var boostedMatch = searcher.Boost(match, 10);

                Span<long> ids = stackalloc long[2048];
                int read = boostedMatch.Fill(ids);
                ids = ids.Slice(0, read);

                Span<float> scores = stackalloc float[ids.Length];
                scores.Fill(1);
                boostedMatch.Score(ids, scores, 1f);

                //When we call 'Boost' on `AllEntries` there is no reason to apply it because it will increase all 'scores' equally and this don't make any sense.                
                for (int i = 0; i < scores.Length; i++)
                    Assert.Equal(1, scores[i]);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void OrBoosting()
        {
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/1", Content1 = 1});
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/11", Content1 = 0});
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/111", Content1 = 0});
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/2", Content1 = 1});
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/4", Content1 = 1});
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/3", Content1 = 2});

            IndexEntries();

            // list/1 matches both startsWith (boost 2x) and Content1='1' → highest score.
            // list/11, list/111 match startsWith only (boost 2x) → second tier.
            // list/2, list/4 match Content1='1' only (no extra boost) → third tier.
            // list/3 (Content1=2) doesn't match either condition → excluded.
            var idsName = ExecuteRQLQueryByScore(
                "FROM TestIndex WHERE boost(startsWith(Id, 'list/1'), 2) OR Content1 = '1' ORDER BY score()");

            Assert.Equal(5, idsName.Count);
            Assert.Equal("list/1", idsName[0]);
            Assert.True(new HashSet<string> {"list/11", "list/111"}.SetEquals(idsName.GetRange(1, 2)));
            Assert.True(new HashSet<string> {"list/2", "list/4"}.SetEquals(idsName.GetRange(3, 2)));

            // Also verify OR-wrapped boost: boost(A OR B, factor) applies the factor to all branches.
            // Score order is the same as above; list/1 (matches both) remains first.
            var idsWrappedBoost = ExecuteRQLQueryByScore(
                "FROM TestIndex WHERE boost(startsWith(Id, 'list/1') OR Content1 = '1', 10) ORDER BY score()");

            Assert.Equal(5, idsWrappedBoost.Count);
            Assert.Equal("list/1", idsWrappedBoost[0]);
            Assert.True(new HashSet<string> {"list/11", "list/111"}.SetEquals(idsWrappedBoost.GetRange(1, 2)));
            Assert.True(new HashSet<string> {"list/2", "list/4"}.SetEquals(idsWrappedBoost.GetRange(3, 2)));
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData(256, 29)]
        [InlineData(512, 29)]
        [InlineData(1024, 29)]
        [InlineData(2048, 29)]
        [InlineData(4096, 31)]
        public void InBoosting(int amount, int mod)
        {
            longList = Enumerable.Range(0, amount).Select(i => new IndexSingleNumericalEntry<long, long> {Id = $"list/{i}", Content1 = i % mod}).ToList();
            IndexEntries();

            // Wrap the IN clause in boost() to exercise score propagation through an IN match.
            var result = ExecuteRQLQueryByScore("FROM TestIndex WHERE boost(Content1 IN ('1', '2', '3'), 2) ORDER BY score()");

            Assert.Equal(result.Count, result.Distinct().Count());

            var localResults = longList.Where(x => x.Content1 is 1 or 2 or 3).Select(y => y.Id).ToArray();
            var highestScore = result.ToArray();
            Array.Sort(localResults);
            Array.Sort(highestScore);
            Assert.True(localResults.SequenceEqual(highestScore));
        }
        
        [RavenFact(RavenTestCategory.Corax)]
        public void OrderByBoosting()
        {
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/1", Content1 = 1});
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/11", Content1 = 0});
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/111", Content1 = 0});
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/2", Content1 = 1});
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/4", Content1 = 1});
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/3", Content1 = 2});

            IndexEntries();

            // boost(startsWith, 20) = inner boost 2 * outer boost 10; boost(Content1='1', 10) = outer boost only.
            // list/1 matches startsWith (boost 20) + Content1='1' (boost 10) → highest.
            // list/11, list/111 match startsWith only (boost 20) → second tier.
            // list/2, list/4 match Content1='1' only (boost 10) → third tier.
            // list/3 matches Content1='2' (no extra boost) → lowest.
            var sortedByCorax = ExecuteRQLQueryByScore(
                "FROM TestIndex WHERE boost(startsWith(Id, 'list/1'), 20) OR boost(Content1 = '1', 10) OR Content1 = '2' ORDER BY score()");

            Assert.Equal(6, sortedByCorax.Count);
            Assert.Equal("list/1", sortedByCorax[0]);
            Assert.True(new HashSet<string> {"list/11", "list/111"}.SetEquals(sortedByCorax.GetRange(1, 2)));
            Assert.True(new HashSet<string> {"list/2", "list/4"}.SetEquals(sortedByCorax.GetRange(3, 2)));
            Assert.Equal("list/3", sortedByCorax[5]);
        }


        [RavenFact(RavenTestCategory.Corax)]
        public void OrderByBoostingTake4()
        {
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/1", Content1 = 1});
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/11", Content1 = 0});
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/111", Content1 = 0});
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/2", Content1 = 1});
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/4", Content1 = 1});
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/3", Content1 = 2});

            IndexEntries();

            // Pass take=4 as a server-side sorter limit to exercise limit propagation through SortingMatch.
            var sortedByCorax = ExecuteRQLQueryByScore(
                "FROM TestIndex WHERE boost(startsWith(Id, 'list/1'), 20) OR boost(Content1 = '1', 10) OR Content1 = '2' ORDER BY score()",
                take: 4);

            Assert.Equal(4, sortedByCorax.Count);
            Assert.Equal("list/1", sortedByCorax[0]);
            Assert.True(new HashSet<string> {"list/11", "list/111"}.SetEquals(sortedByCorax.GetRange(1, 2)));
            Assert.True(sortedByCorax[3] == "list/2" || sortedByCorax[3] == "list/4");
        }

        private static int CompareAscending(IndexSingleNumericalEntry<long, long> value1, IndexSingleNumericalEntry<long, long> value2)
        {
            return value1.Content1.CompareTo(value2.Content1);
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void OrderByBoostingTermFrequency()
        {
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/1", Content1 = 0}); // 1/2
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/2", Content1 = 0}); // 1/2
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/3", Content1 = 1}); // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/4", Content1 = 1}); // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/5", Content1 = 1}); // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/6", Content1 = 1}); // 1/4

            IndexEntries();

            // BM25 IDF: Content1=0 (2 entries) scores higher than Content1=1 (4 entries).
            var sortedByCorax = ExecuteRQLQueryByScore(
                "FROM TestIndex WHERE Content1 = '0' OR Content1 = '1' ORDER BY score()");

            for (int i = 0; i < longList.Count; ++i)
            {
                if (longList[i].Id != sortedByCorax[i])
                {
                    // Since documents can change places (unstable sort), verify the Content1 group is correct.
                    var originalEntry = longList.Single(isne => isne.Id == sortedByCorax[i]);
                    Assert.Equal(longList[i].Content1, originalEntry.Content1);
                }
                else
                    Assert.Equal(longList[i].Id, sortedByCorax[i]);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void OrderByBoostingOrBasedInQuery()
        {
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/1", Content1 = 0}); // 1/2
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/2", Content1 = 0}); // 1/2
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/3", Content1 = 1}); // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/4", Content1 = 1}); // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/5", Content1 = 1}); // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/6", Content1 = 1}); // 1/4

            IndexEntries();

            // BM25 IDF: Content1=0 (2 entries) scores higher than Content1=1 (4 entries).
            var sortedByCorax = ExecuteRQLQueryByScore(
                "FROM TestIndex WHERE Content1 IN ('0', '1') ORDER BY score()");

            for (int i = 0; i < longList.Count; ++i)
            {
                if (longList[i].Id != sortedByCorax[i])
                {
                    // Since documents can change places (unstable sort), verify the Content1 group is correct.
                    var originalEntry = longList.Single(isne => isne.Id == sortedByCorax[i]);
                    Assert.Equal(longList[i].Content1, originalEntry.Content1);
                }
                else
                    Assert.Equal(longList[i].Id, sortedByCorax[i]);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public unsafe void CanAddAndRemoveDocumentBoost()
        {
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/1", Content1 = 0, Boost = 10F}); // 1            
            IndexEntries();
            var ids = new long[16];
            
            using(var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator)))
            {
                var read = searcher.AllEntries().Fill(ids);
                Assert.True(searcher.DocumentsAreBoosted);
                Assert.Equal(1, read);
                var boostTree = searcher.GetDocumentBoostTree();
                var boost = (float*)boostTree.ReadPtr(ids[0], out var _);
                Assert.Equal(2.39789534, *boost, 0.01f);
            }
            
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var knownFields = CreateKnownFields(bsc);
            using (var indexWriter = new IndexWriter(Env, knownFields, SupportedFeatures.All))
            {
                indexWriter.TryDeleteEntry("list/1");
                indexWriter.Commit();
            }
            
            using(var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator)))
            {
                var read = searcher.AllEntries().Fill(ids);
                Assert.False(searcher.DocumentsAreBoosted);
                Assert.Equal(0, read);
                var boostTree = searcher.GetDocumentBoostTree();
                Assert.Equal(0, boostTree.NumberOfEntries);
            }
        }
        
        [RavenFact(RavenTestCategory.Corax)]
        public void OrderByBoostingMultiTermFrequency()
        {
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/1", Content1 = 0}); // 1            
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/3", Content1 = 2}); // 1/3
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/4", Content1 = 3}); // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/2", Content1 = 1}); // 1/2
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/22", Content1 = 1}); // 1/2
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/33", Content1 = 2}); // 1/3
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/44", Content1 = 3}); // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/333", Content1 = 2}); // 1/3
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/444", Content1 = 3}); // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> {Id = $"list/4444", Content1 = 3}); // 1/4

            IndexEntries();
            longList.Sort(CompareAscending);

            // BM25 IDF: Content1=0 (1 entry) > Content1=1 (2) > Content1=2 (3) > Content1=3 (4 entries).
            // After sorting longList ascending by Content1, sortedByCorax[i].Content1 must equal longList[i].Content1.
            var sortedByCorax = ExecuteRQLQueryByScoreReadContent1(
                "FROM TestIndex WHERE Content1 IN ('0', '1', '2', '3') ORDER BY score()");

            for (int i = 0; i < longList.Count; ++i)
                Assert.Equal(longList[i].Content1, sortedByCorax[i]);
        }

        private List<string> ExecuteRQLQueryByScore(string rqlQuery, long take = long.MaxValue)
        {
            using var knownFields = CreateKnownFields(Allocator);
            using var searcher = new IndexSearcher(Env, knownFields);
            var queryMetadata = new QueryMetadata(rqlQuery, null, 0);
            var planParams = new PlanParameters
            {
                IndexSearcher = searcher,
                Metadata = queryMetadata,
                HasBoost = true,
                Allocator = Allocator
            };
            var match = QueryPlanBuilder.BuildFilterMatch(planParams, new QueryBuilderParameters(searcher, Allocator, queryMetadata, null, knownFields, hasBoost: true), null, false, default);
            match = ApplyScoreOrderingIfRequested(searcher, queryMetadata, match, take);
            var list = new List<string>();
            Span<long> ids = stackalloc long[256];
            int count;
            while ((count = match.Fill(ids)) > 0)
                for (int i = 0; i < count; i++)
                    list.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(ids[i]));
            return list;
        }

        private List<long> ExecuteRQLQueryByScoreReadContent1(string rqlQuery)
        {
            using var knownFields = CreateKnownFields(Allocator);
            using var searcher = new IndexSearcher(Env, knownFields);
            var queryMetadata = new QueryMetadata(rqlQuery, null, 0);
            var planParams = new PlanParameters
            {
                IndexSearcher = searcher,
                Metadata = queryMetadata,
                HasBoost = true,
                Allocator = Allocator
            };
            var match = QueryPlanBuilder.BuildFilterMatch(planParams, new QueryBuilderParameters(searcher, Allocator, queryMetadata, null, knownFields, hasBoost: true), null, false, default);
            match = ApplyScoreOrderingIfRequested(searcher, queryMetadata, match, long.MaxValue);
            var list = new List<long>();
            var termsReader = searcher.TermsReaderFor("Content1");
            Span<long> ids = stackalloc long[256];
            int count;
            while ((count = match.Fill(ids)) > 0)
                for (int i = 0; i < count; i++)
                    list.Add(long.Parse(termsReader.GetTermFor(ids[i])));
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

        private void PrepareData(bool inverse = false)
        {
            for (int i = 0; i < 1000; ++i)
            {
                longList.Add(new IndexSingleNumericalEntry<long, long> {Id = inverse ? $"list/1000-{i}" : $"list/{i}", Content1 = i, Content2 = inverse ? 1000 - i : i});
            }
        }

        private void IndexEntries()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var knownFields = CreateKnownFields(bsc);

            using var indexWriter = new IndexWriter(Env, knownFields, SupportedFeatures.All);

            foreach (var entry in longList)
            {
                using var builder = indexWriter.Index(Encoding.UTF8.GetBytes(entry.Id));
                builder.Write(IndexId, null, Encoding.UTF8.GetBytes(entry.Id));
                builder.Write(Content1, null, Encoding.UTF8.GetBytes(entry.Content1.ToString()), entry.Content1, Convert.ToDouble(entry.Content1));
                builder.Write(Content2, null, Encoding.UTF8.GetBytes(entry.Content2.ToString()), entry.Content2, Convert.ToDouble(entry.Content2));

                if (entry.Boost.HasValue)
                    builder.Boost(entry.Boost.Value);
                builder.EndWriting();
            }

            indexWriter.Commit();
        }

        private IndexFieldsMapping CreateKnownFields(ByteStringContext bsc)
        {
            Slice.From(bsc, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(bsc, "Content1", ByteStringType.Immutable, out Slice content1Slice);
            Slice.From(bsc, "Content2", ByteStringType.Immutable, out Slice content2Slice);

            using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
                .AddBinding(IndexId, idSlice)
                .AddBinding(Content1, content1Slice)
                .AddBinding(Content2, content2Slice);

            return builder.Build();
        }

        private class IndexSingleNumericalEntry<T1, T2>
        {
            public string Id { get; set; }
            public T1 Content1 { get; set; }
            public T2 Content2 { get; set; }
            public float? Boost { get; set; }
        }
    }
}
