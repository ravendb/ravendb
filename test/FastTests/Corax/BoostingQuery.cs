using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax;
using Corax.Mappings;
using Corax.Queries;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;
using Xunit.Abstractions;
using static Corax.Queries.SortingMatch;

namespace FastTests.Corax
{
    public class BoostingQueryTest : StorageTest
    {
        private List<IndexSingleNumericalEntry<long, long>> longList = new();
        private const int IndexId = 0, Content1 = 1, Content2 = 2;

        public BoostingQueryTest(ITestOutputHelper output) : base(output) { }


        [Fact]
        public void SimpleBoosting()
        {
            PrepareData();            
            IndexEntries();         
            using var searcher = new IndexSearcher(Env);
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

        [Fact]
        public void OrBoosting()
        {
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/1", Content1 = 1 });   //  2 * 10
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/11", Content1 = 0 });  //  2 
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/111", Content1 = 0 }); //  2
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/2", Content1 = 1 });   //  10            
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/4", Content1 = 1 });   //  10
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/3", Content1 = 2 });    

            IndexEntries();
            using var searcher = new IndexSearcher(Env);
            {
                var startWithMatch = searcher.StartWithQuery(searcher.FieldMetadataBuilder("Id", hasBoost: true), "list/1");
                var boostedStartWithMatch = searcher.Boost(startWithMatch, 2);
                var contentMatch = searcher.TermQuery(searcher.FieldMetadataBuilder("Content1", hasBoost: true), "1");
                var orMatch = searcher.Or(boostedStartWithMatch, contentMatch);
                var boostedOrMatch = searcher.Boost(orMatch, 10);
                var orderByScore = searcher.OrderByScore(boostedOrMatch);
                Span<long> ids = stackalloc long[2048];
                int read = orderByScore.Fill(ids);
                ids = ids.Slice(0, read);
                
                List<string> idsName = new();
                for (int i = 0; i < read; ++i)
                    idsName.Add(searcher.GetIdentityFor(ids[i]));
                
                Assert.Equal("list/1", idsName[0]); // most unique 
                Assert.Equal("list/11", idsName[1]); // 2nd
                Assert.Equal("list/111", idsName[2]); // 3th
                Assert.Equal("list/2", idsName[3]); // those scoring has no impact
                Assert.Equal("list/4", idsName[4]); //
            }
        }

        [Theory]
        [InlineData(256, 29)]
        [InlineData(512, 29)]
        [InlineData(1024, 29)]
        [InlineData(2048, 29)]
        [InlineData(4096, 31)]
        public void InBoosting(int amount, int mod)
        {
            longList = Enumerable.Range(0, amount).Select(i => new IndexSingleNumericalEntry<long, long> { Id = $"list/{i}", Content1 = i % mod }).ToList();
            IndexEntries();
            using var searcher = new IndexSearcher(Env);
            var contentMetadata = searcher.FieldMetadataBuilder("Content1", hasBoost: true);
            {
                IQueryMatch match =searcher.Boost(
                    searcher.InQuery(contentMetadata, new() { "1", "2", "3" })
                    , 10);
                
                match = searcher.OrderByScore(match);
                Span<long> ids = stackalloc long[amount];
                var read = match.Fill(ids);
                List<string> result = new();
                for (int i = 0; i < read; ++i)
                {
                    result.Add(searcher.GetIdentityFor(ids[i]));
                }

                Assert.Equal(result.Count, result.Distinct().Count());

                var localResults = longList.Where(x => x.Content1 is 1 or 2 or 3).Select(y => y.Id).ToArray();
                var highestScore = result.ToArray().AsSpan(0, localResults.Length).ToArray();
                Array.Sort(localResults);
                Array.Sort(highestScore);
                Assert.True(localResults.SequenceEqual(highestScore));
            }
        }

        [Theory]
        [InlineData(256, 29)]
        [InlineData(512, 29)]
        [InlineData(1024, 29)]
        [InlineData(2048, 29)]
        [InlineData(4096, 31)]
        public void UnaryBoostingTest(int amount, int mod)
        {
            longList = Enumerable.Range(0, amount).Select(i => new IndexSingleNumericalEntry<long, long> { Id = $"list/{i}", Content1 = i % mod }).ToList();
            IndexEntries();
            using var searcher = new IndexSearcher(Env);
            var contentMetadata = searcher.FieldMetadataBuilder("Content", 1);
            {
                var match = searcher.Boost(searcher.UnaryQuery(searcher.AllEntries(), contentMetadata, 10L, UnaryMatchOperation.GreaterThan), 100);
                Span<long> ids = stackalloc long[amount];
                var read = match.Fill(ids);
                // Assert.Equal(longList.Count, read);
                List<string> result = new();
                for (int i = 0; i < read; ++i)
                {
                    result.Add(searcher.GetIdentityFor(ids[i]));
                }

                Assert.Equal(result.Count, result.Distinct().Count());

                var localResults = longList.Where(x => x.Content1 > 10).Select(y => y.Id).ToArray();
                var highestScore = result.ToArray().AsSpan(0, localResults.Length).ToArray();
                Array.Sort(localResults);
                Array.Sort(highestScore);

                Assert.True(localResults.SequenceEqual(highestScore));
            }
        }

        [Fact]
        public void OrderByBoosting()
        {
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/1", Content1 = 1 }); // 2 * 10
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/11", Content1 = 0 }); // 2 * 10
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/111", Content1 = 0 }); // 2 * 10
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/2", Content1 = 1 }); //     10
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/4", Content1 = 1 }); //     10
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/3", Content1 = 2 }); //      0

            IndexEntries();
            using var searcher = new IndexSearcher(Env);
            {
                var startWithMatch = searcher.StartWithQuery("Id", "list/1");
                var boostedStartWithMatch = searcher.Boost(startWithMatch, 2);
                var contentMatch = searcher.TermQuery("Content1", "1");
                var orMatch = searcher.Or(boostedStartWithMatch, contentMatch);
                var boostedOrMatch = searcher.Boost(orMatch, 10);
                var contentMatch2 = searcher.TermQuery("Content1", "2");
                var orMatch2 = searcher.Or(contentMatch2, boostedOrMatch);
                var sortedMatch = searcher.OrderByScore(orMatch2);

                Span<long> ids = stackalloc long[2048];
                int read = sortedMatch.Fill(ids);
                ids = ids.Slice(0, read);

                List<string> sortedByCorax = new();
                for (int i = 0; i < read; ++i)
                    sortedByCorax.Add(searcher.GetIdentityFor(ids[i]));

                for (int i = 0; i < longList.Count; ++i)
                    Assert.Equal(longList[i].Id, sortedByCorax[i]);
            }
        }


        [Fact]
        public void OrderByBoostingTake4()
        {
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/1", Content1 = 1 });   // 2 * 10
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/11", Content1 = 0 });  // 2 * 10
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/111", Content1 = 0 }); // 2 * 10
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/2", Content1 = 1 });   //     10
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/4", Content1 = 1 });   //     10
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/3", Content1 = 2 });   //      0

            IndexEntries();
            using var searcher = new IndexSearcher(Env);
            {
                var startWithMatch = searcher.StartWithQuery("Id", "list/1");
                var boostedStartWithMatch = searcher.Boost(startWithMatch, 2);
                var contentMatch = searcher.TermQuery("Content1", "1");
                var orMatch = searcher.Or(boostedStartWithMatch, contentMatch);
                var boostedOrMatch = searcher.Boost(orMatch, 10);
                var contentMatch2 = searcher.TermQuery("Content1", "2");
                var orMatch2 = searcher.Or(contentMatch2, boostedOrMatch);
                var sortedMatch = searcher.OrderByScore(orMatch2, 4);

                // TODO: Check what happens in OrderBy statements when the buffer is too small. 
                Span<long> ids = stackalloc long[1024];

                var read = sortedMatch.Fill(ids);

                List<string> sortedByCorax = new();
                for (int i = 0; i < read; ++i)
                    sortedByCorax.Add(searcher.GetIdentityFor(ids[i]));

                for (int i = 0; i < 4; ++i)
                    Assert.Equal(longList[i].Id, sortedByCorax[i]);
            }
        }

        private static int CompareAscending(IndexSingleNumericalEntry<long, long> value1, IndexSingleNumericalEntry<long, long> value2)
        {
            return value1.Content1.CompareTo(value2.Content1);
        }

        [Fact]
        public void OrderByBoostingTermFrequency()
        {
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/1", Content1 = 0 });   // 1/2
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/2", Content1 = 0 });   // 1/2
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/3", Content1 = 1 });   // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/4", Content1 = 1 });   // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/5", Content1 = 1 });   // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/6", Content1 = 1 });   // 1/4            

            IndexEntries();
            using var searcher = new IndexSearcher(Env);
            {
                var content0Match = searcher.TermQuery("Content1", "0", hasBoost: true);
                var boostedContent0 = searcher.Boost(content0Match, 0);

                var content1Match = searcher.TermQuery("Content1", "1", hasBoost: true);
                var boostedContent1 = searcher.Boost(content1Match, 0);

                var orMatch = searcher.Or(boostedContent0, boostedContent1);
                var boostedOrMatch = searcher.Boost(orMatch, 10);
                var sortedMatch = searcher.OrderByScore(boostedOrMatch);

                Span<long> ids = stackalloc long[1024];
                var read = sortedMatch.Fill(ids);

                List<string> sortedByCorax = new();
                for (int i = 0; i < read; ++i)
                    sortedByCorax.Add(searcher.GetIdentityFor(ids[i]));

                for (int i = 0; i < longList.Count; ++i)
                    Assert.Equal(longList[i].Id, sortedByCorax[i]);
            }
        }

        //todo
        [Fact]
        public void OrderByBoostingOrBasedInQuery()
        {
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/1", Content1 = 0 });   // 1/2
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/2", Content1 = 0 });   // 1/2
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/3", Content1 = 1 });   // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/4", Content1 = 1 });   // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/5", Content1 = 1 });   // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/6", Content1 = 1 });   // 1/4


            IndexEntries();
            using var searcher = new IndexSearcher(Env);
            var contentMetadata = searcher.FieldMetadataBuilder("Content1");
            {
                var query = searcher.OrderByScore(searcher.InQuery(contentMetadata, new List<string>() { "0", "1" }));

                Span<long> ids = stackalloc long[1024];
                var read = query.Fill(ids);

                List<string> sortedByCorax = new();
                for (int i = 0; i < read; ++i)
                    sortedByCorax.Add(searcher.GetIdentityFor(ids[i]));

                for (int i = 0; i < longList.Count; ++i)
                    Assert.Equal(longList[i].Id, sortedByCorax[i]);
            }
        }

        
        [Fact]
        public void OrderByBoostingMultiTermFrequency()
        {
        
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/1", Content1 = 0 });   // 1            
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/3", Content1 = 2 });   // 1/3
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/4", Content1 = 3 });   // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/2", Content1 = 1 });   // 1/2
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/22", Content1 = 1 });   // 1/2
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/33", Content1 = 2 });   // 1/3
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/44", Content1 = 3 });   // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/333", Content1 = 2 });   // 1/3
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/444", Content1 = 3 });   // 1/4
            longList.Add(new IndexSingleNumericalEntry<long, long> { Id = $"list/4444", Content1 = 3 });   // 1/4
        
            IndexEntries();
        
            longList.Sort(CompareAscending);
            using var searcher = new IndexSearcher(Env);
            var contentMetadata = searcher.FieldMetadataBuilder("Content1", Content1, hasBoost: true);
            {
                var query = searcher.InQuery(contentMetadata, new List<string>() {"0", "1", "2", "3"});
                var sortedMatch = searcher.OrderByScore(query);
        
                Span<long> ids = stackalloc long[1024];
                var read = sortedMatch.Fill(ids);
        
                List<long> sortedByCorax = new();
                for (int i = 0; i < read; ++i)
                {
                    searcher.GetEntryReaderFor(ids[i]).GetFieldReaderFor(Content1).Read(out long value);
                    sortedByCorax.Add(value);
                }                    
        
                for (int i = 0; i < longList.Count; ++i)
                    Assert.Equal(longList[i].Content1, sortedByCorax[i]);
            }
        }

        private void PrepareData(bool inverse = false)
        {
            for (int i = 0; i < 1000; ++i)
            {
                longList.Add(new IndexSingleNumericalEntry<long, long>
                {
                    Id = inverse ? $"list/1000-{i}" : $"list/{i}",
                    Content1 = i,
                    Content2 = inverse ? 1000 - i : i
                });
            }
        }

        private void IndexEntries()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var knownFields = CreateKnownFields(bsc);

            using var indexWriter = new IndexWriter(Env, knownFields);
            var entryWriter = new IndexEntryWriter(bsc, knownFields);

            foreach (var entry in longList)
            {
                using var __ = CreateIndexEntry(ref entryWriter, entry, out var data);
                indexWriter.Index(entry.Id, data.ToSpan());
            }
            indexWriter.Commit();
        }

        private ByteStringContext<ByteStringMemoryCache>.InternalScope CreateIndexEntry(
            ref IndexEntryWriter entryWriter, IndexSingleNumericalEntry<long, long> entry, out ByteString output)
        {
            entryWriter.Write(IndexId, Encoding.UTF8.GetBytes(entry.Id));
            entryWriter.Write(Content1, Encoding.UTF8.GetBytes(entry.Content1.ToString()), entry.Content1, Convert.ToDouble(entry.Content1));
            entryWriter.Write(Content2, Encoding.UTF8.GetBytes(entry.Content2.ToString()), entry.Content2, Convert.ToDouble(entry.Content2));
            return entryWriter.Finish(out output);
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
        }
    }
}
