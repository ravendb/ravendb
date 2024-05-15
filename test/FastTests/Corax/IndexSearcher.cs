using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using Corax;
using Corax.Analyzers;
using Corax.Querying;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using FastTests.Voron;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;
using VoronConstants = Voron.Global.Constants;

namespace FastTests.Corax
{
    public class IndexSearcherTest : StorageTest
    {
        public IndexSearcherTest(ITestOutputHelper output) : base(output)
        {
        }
        [Fact]
        public void CanDeleteDifferentLongAndDoubleInSingleEntry()
        {
            var entry1 = new IndexSingleEntry() {Id = "e/1", Content = "2023-08-02T12:01:34.2111452"};
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            var knownFields = CreateKnownFields(bsc);
            using (var indexWriter = new IndexWriter(Env, knownFields, SupportedFeatures.All))
            {
                using (var builder = indexWriter.Index(entry1.Id))
                {
                    builder.Write(IdIndex, PrepareString(entry1.Id));
                    var dateTime = DateTime.Parse(entry1.Content);
                    builder.Write(ContentIndex, Encodings.Utf8.GetBytes(entry1.Content), dateTime.Ticks, dateTime.Ticks);
                    double doubleVal = dateTime.Ticks;
                    Assert.NotEqual(dateTime.Ticks, (long)doubleVal);
                }

                indexWriter.Commit();
            }

            using (var indexWriter = new IndexWriter(Env, knownFields, SupportedFeatures.All))
            {
                Assert.True(indexWriter.TryDeleteEntry("e/1"));
                indexWriter.Commit();
            }

            using (var indexSearcher = new IndexSearcher(Env, knownFields))
            {
                Assert.True(knownFields.TryGetByFieldId(ContentIndex, out var binding));
                var query = indexSearcher.BetweenQuery(binding.Metadata, double.MinValue, double.MaxValue, UnaryMatchOperation.GreaterThanOrEqual,
                    UnaryMatchOperation.LessThanOrEqual);
                Span<long> ids = stackalloc long[64];

                Assert.Equal(0, query.Fill(ids));
            }            
        }
        
        [Fact]
        public void GetTermFromEntryIdViaEntriesFields()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"muddy", "road"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                Span<long> ids = stackalloc long[16];

                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match = searcher.TermQuery("Content", "road");
                Assert.Equal(2, match.Count);
                Assert.Equal(2, match.Fill(ids));
                using var reader = searcher.TermsReaderFor("Content");
                Assert.True(reader.TryGetTermFor(ids[0], out string term));
                Assert.Equal("lake", term);
                Assert.True(reader.TryGetTermFor(ids[1], out  term));
                Assert.Equal("muddy", term);
            }
        }
        
        [Fact]
        public void CanCompareEntriesDirectly()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"muddy", "road"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                Span<long> ids = stackalloc long[16];

                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match = searcher.TermQuery("Content", "road");
                Assert.Equal(2, match.Count);
                Assert.Equal(2, match.Fill(ids));
                using var reader = searcher.TermsReaderFor("Content");
                Assert.True(ids[0] < ids[1]);

                var term0 = entry1.Content.OrderBy(x => x).First();
                var term1 = entry2.Content.OrderBy(x => x).First();
                
                
                var cmp = reader.Compare(ids[0], ids[1]);
                Assert.Equal(string.Compare(term0, term1, StringComparison.Ordinal),Math.Sign(cmp));
                cmp = reader.Compare(ids[1], ids[0]);
                Assert.Equal(string.Compare(term1, term0, StringComparison.Ordinal), Math.Sign(cmp));
                cmp = reader.Compare(ids[0], ids[0]);
                Assert.Equal(string.Compare(term0, term0, StringComparison.Ordinal), Math.Sign(cmp));
                cmp = reader.Compare(ids[1], ids[1]);
                Assert.Equal(string.Compare(term1, term1, StringComparison.Ordinal), Math.Sign(cmp));
            }
        }

        [Fact]
        public void EmptyTerm()
        {
            var entry = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry}, CreateKnownFields(bsc));

            {
                Span<long> ids = stackalloc long[16];

                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match = searcher.TermQuery("Unknown", "1");
                Assert.Equal(0, match.Count);
                Assert.Equal(0, match.Fill(ids));

                match = searcher.TermQuery("Id", "1");
                Assert.Equal(0, match.Count);
                Assert.Equal(0, match.Fill(ids));
            }
        }

        [Fact]
        public void SingleTerm()
        {
            var entry = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry}, CreateKnownFields(bsc));

            {
                Span<long> ids = stackalloc long[16];

                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match = searcher.TermQuery("Id", "entry/1");
                Assert.Equal(1, match.Count);
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }
        }

        [Fact]
        public void SmallSetTerm()
        {
            var entries = new IndexEntry[16];
            for (int i = 0; i < entries.Length; i++)
            {
                entries[i] = new IndexEntry {Id = $"entry/{i}", Content = new string[] {"road"},};
            }

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entries, CreateKnownFields(bsc));

            {
                Span<long> ids = stackalloc long[12];
                ids.Fill(-1);

                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match = searcher.TermQuery("Content", "road");

                Assert.Equal(16, match.Count);

                Assert.Equal(12, match.Fill(ids));
                Assert.False(ids.Contains(-1));

                ids.Fill(-1);
                Assert.Equal(4, match.Fill(ids));
                Assert.True(ids.Contains(-1));

                Assert.Equal(0, match.Fill(ids));
            }
        }

        [Fact]
        public void EmptyAnd()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match1 = searcher.TermQuery("Id", "entry/1");
                var match2 = searcher.TermQuery("Content", "mountain");
                var andMatch = searcher.And(in match1, in match2);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(0, andMatch.Fill(ids));
            }
        }

        [Fact]
        public void SingleAndNoDuplication()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};

            
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match1 = searcher.InQuery("Content", new List<string>() {"road", "lake"});
                var match2 = searcher.ExistsQuery(searcher.FieldMetadataBuilder("Content"));
                var andMatch = searcher.And(in match1, in match2);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, andMatch.Fill(ids));
                Assert.Equal(0, andMatch.Fill(ids));
            }
        }

        [Fact]
        public void SingleAnd()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match1 = searcher.TermQuery("Id", "entry/1");
                var match2 = searcher.TermQuery("Content", "mountain");
                var andMatch = searcher.And(in match1, in match2);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, andMatch.Fill(ids));
                Assert.Equal(0, andMatch.Fill(ids));
            }
        }

        [Fact]
        public void AllAnd()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake", "mountain"},};
            var entry2 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match1 = searcher.TermQuery("Id", "entry/1");
                var match2 = searcher.TermQuery("Content", "mountain");
                var andMatch = searcher.And(in match1, in match2);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, andMatch.Fill(ids));
                Assert.Equal(0, andMatch.Fill(ids));
            }
        }

        [Fact]
        public void AllAndWithEmpty()
        {
            var entries = Enumerable.Range(1, 10_000).Select(i => new IndexEntry {Id = $"entry/{i}", Content = new string[] {"road", "lake", "mountain"}}).ToArray();


            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entries, CreateKnownFields(bsc));

            {
                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match1 = searcher.AllEntries();
                var match2 = searcher.TermQuery("Id", "Maciej");
                var andMatch = searcher.And(in match1, in match2);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(0, andMatch.Fill(ids));
            }
        }

        [Fact]
        public void AllAndMemoized()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake", "mountain"},};
            var entry2 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

                var match1 = searcher.Memoize(searcher.TermQuery("Id", "entry/1"));
                var match2 = searcher.Memoize(searcher.TermQuery("Content", "mountain"));
                var andMatch = searcher.Memoize(searcher.And(match1.Replay(), match2.Replay()));

                var replay1 = andMatch.Replay();
                var replay2 = andMatch.Replay();

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, replay1.Fill(ids));
                Assert.Equal(0, replay1.Fill(ids));
                Assert.Equal(2, replay2.Fill(ids));
                Assert.Equal(0, replay2.Fill(ids));
            }
        }

        [Fact]
        public void EmptyOr()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match1 = searcher.TermQuery("Id", "entry/3");
                var match2 = searcher.TermQuery("Content", "highway");
                var orMatch = searcher.Or(in match1, in match2);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(0, orMatch.Fill(ids));
                Assert.Equal(0, orMatch.Fill(ids));
            }
        }

        [Fact]
        public void SingleOr()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                Span<long> ids = stackalloc long[16];

                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match1 = searcher.TermQuery("Id", "entry/1");
                var match2 = searcher.TermQuery("Content", "highway");
                var orMatch = searcher.Or(in match1, in match2);

                Assert.Equal(1, orMatch.Fill(ids));
                Assert.Equal(0, orMatch.Fill(ids));

                match1 = searcher.TermQuery("Id", "entry/3");
                match2 = searcher.TermQuery("Content", "mountain");
                orMatch = searcher.Or(in match1, in match2);

                Assert.Equal(1, orMatch.Fill(ids));
                Assert.Equal(0, orMatch.Fill(ids));
            }
        }

        [Fact]
        public void AllOr()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match1 = searcher.TermQuery("Id", "entry/1");
                var match2 = searcher.TermQuery("Content", "mountain");
                var orMatch = searcher.Or(in match1, in match2);

                Span<long> ids = stackalloc long[16];

                Assert.Equal(2, orMatch.Fill(ids));
                Assert.Equal(0, orMatch.Fill(ids));
            }
        }

        [Fact]
        public void AllOrInBatches()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};
            var entry3 = new IndexEntry {Id = "entry/3", Content = new string[] {"trail", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            {
                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match1 = searcher.TermQuery("Id", "entry/1");
                var match2 = searcher.TermQuery("Content", "mountain");
                var orMatch = searcher.Or(in match1, in match2);

                Span<long> ids = stackalloc long[2];
                Assert.Equal(2, orMatch.Fill(ids));
                Assert.Equal(1, orMatch.Fill(ids));
                Assert.Equal(0, orMatch.Fill(ids));
            }
        }

        [Fact]
        public void SimpleAndOr()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake", "mountain"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};
            var entry3 = new IndexEntry {Id = "entry/3", Content = new string[] {"sky", "space"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            {
                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match1 = searcher.TermQuery("Id", "entry/1");
                var match2 = searcher.TermQuery("Content", "mountain");
                var andMatch = searcher.And(in match1, in match2);
                var match3 = searcher.TermQuery("Id", "entry/3");
                var orMatch = searcher.Or(in andMatch, in match3);


                Span<long> ids = stackalloc long[8];
                Assert.Equal(2, orMatch.Fill(ids));
                Assert.Equal(0, orMatch.Fill(ids));
            }

            {
                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match1 = searcher.TermQuery("Id", "entry/1");
                var match2 = searcher.TermQuery("Content", "mountain");
                var andMatch = searcher.And(in match1, in match2);
                var match3 = searcher.TermQuery("Id", "entry/3");
                var orMatch = searcher.Or(in match3, in andMatch);

                Span<long> ids = stackalloc long[8];
                Assert.Equal(2, orMatch.Fill(ids));
                Assert.Equal(0, orMatch.Fill(ids));
            }
        }


        [Theory]
        [InlineData(new object[] {10, 3})]
        [InlineData(new object[] {8000, 18})]
        [InlineData(new object[] {1000, 8})]
        [InlineData(new object[] {1020, 7})]
        [InlineData(new object[] {201, 128})]
        public void SimpleAndOrForBiggerSet(int setSize, int stackSize)
        {
            setSize = setSize - (setSize % 3);
            var matches = new List<IndexEntry>();

            var entriesToIndex = new IndexEntry[setSize];
            for (int i = 0; i < setSize; i++)
            {
                var entry = new IndexEntry
                {
                    Id = $"entry/{i}",
                    Content = (i % 3) switch
                    {
                        0 => new string[] {"road", "lake", "mountain"},
                        1 => new string[] {"road", "mountain"},
                        2 => new string[] {"sky", "space", "lake"},
                        _ => throw new InvalidDataException("This should not happen.")
                    }
                };

                if (entry.Content.Contains("lake") && entry.Content.Contains("mountain") || entry.Content.Contains("space"))
                {
                    matches.Add(entry);
                }

                entriesToIndex[i] = entry;
            }

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entriesToIndex, CreateKnownFields(bsc));

            var matchesId = matches.Select(x => x.IndexEntryId).ToList();
            matchesId.Sort();
            {
                using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
                var match1 = searcher.TermQuery("Content", "lake");
                var match2 = searcher.TermQuery("Content", "mountain");
                var andMatch = searcher.And(in match1, in match2);
                var match3 = searcher.TermQuery("Content", "space");
                var orMatch = searcher.Or(in andMatch, in match3);

                var actual = new List<long>();
                Span<long> ids = stackalloc long[stackSize];
                int read;
                int count = 0;
                do
                {
                    read = orMatch.Fill(ids);
                    count += read;
                    actual.AddRange(ids[..read].ToArray());
                } while (read != 0);

                // Because there is no guarantee that multiple Fill operations would return sequential non redundant document ids,
                // we need to sort and remove duplicates before actually testing the final condition. 
                var sortedActual = actual.ToArray();
                var uniqueIdsCount = Sorting.SortAndRemoveDuplicates(sortedActual.AsSpan());
                var uniqueIds = sortedActual.AsSpan().Slice(0, uniqueIdsCount);
                
                Assert.Equal(matchesId.Count, uniqueIdsCount);
                
                for (int i = 0; i < uniqueIdsCount; i++)
                {
                    Assert.Equal(matchesId[i], uniqueIds[i]);
                }

                Assert.Equal((setSize / 3) * 2, count);
            }
        }

        [Fact]
        public void SimpleInStatement()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake", "mountain"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};
            var entry3 = new IndexEntry {Id = "entry/3", Content = new string[] {"sky", "space"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            {
                var match = searcher.InQuery("Content", new() {"road", "space"});

                Span<long> ids = stackalloc long[2];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.InQuery("Content", new() {"road", "space"});

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.InQuery("Content", new() {"sky", "space"});

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.InQuery("Content", new() {"road", "mountain", "space"});

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }
        }

        [Theory]
        [InlineData(new object[] {1000, 8})]
        [InlineData(new object[] {300, 128})]
        [InlineData(new object[] {10000, 128})]
        public void AndInStatement(int setSize, int stackSize)
        {
            setSize = setSize - (setSize % 3);

            var matches = new List<IndexEntry>();
            var entriesToIndex = new IndexEntry[setSize];
            for (int i = 0; i < setSize; i++)
            {
                var entry = new IndexEntry
                {
                    Id = $"entry/{i}",
                    Content = (i % 3) switch
                    {
                        0 => new string[] {"road", "lake", "mountain"},
                        1 => new string[] {"road", "mountain"},
                        2 => new string[] {"sky", "space", "lake"},
                        _ => throw new InvalidDataException("This should not happen.")
                    }
                };

                entriesToIndex[i] = entry;
                if ((entry.Content.Contains("lake") || entry.Content.Contains("mountain")) && entry.Content.Contains("sky"))
                {
                    matches.Add(entry);
                }
            }

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entriesToIndex, CreateKnownFields(bsc));

            var matchIds = matches.Select(x => x.IndexEntryId).ToArray();

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            {
                var match1 = searcher.InQuery("Content", new() {"lake", "mountain"});
                var match2 = searcher.TermQuery("Content", "sky");
                var andMatch = searcher.And(in match1, in match2);

                var actual = new List<long>();
                Span<long> ids = stackalloc long[stackSize];
                int read;
                do
                {
                    read = andMatch.Fill(ids);
                    actual.AddRange(ids[..read].ToArray());
                } while (read != 0);

                var actualSorted = actual.ToArray();
                var actualSize = Sorting.SortAndRemoveDuplicates(actualSorted.AsSpan());

                for (int i = 0; i < actualSize; i++)
                {
                    Assert.Equal(matchIds[i], actualSorted[i]);
                }

                Assert.Equal((setSize / 3), actualSize);
            }

            {
                var match1 = searcher.TermQuery("Content", "sky");
                var match2 = searcher.InQuery("Content", new() {"lake", "mountain"});
                var andMatch = searcher.And(in match1, in match2);

                var actual = new List<long>();
                Span<long> ids = stackalloc long[stackSize];
                int read;
                int count = 0;
                do
                {
                    read = andMatch.Fill(ids);
                    actual.AddRange(ids[..read].ToArray());
                    count += read;
                } while (read != 0);

                var actualSorted = actual.ToArray();
                var actualSize = Sorting.SortAndRemoveDuplicates(actualSorted.AsSpan());

                for (int i = 0; i < actualSize; i++)
                {
                    Assert.Equal(matchIds[i], actualSorted[i]);
                }

                Assert.Equal((setSize / 3), actualSize);
            }
        }

        [Fact]
        public void AllIn()
        {
            var entry0 = new IndexEntry
            {
                Id = "entry/0",
                Content = new string[]
                {
                    "quo", "consequatur?", "officia", "in", "pariatur.", "illo", "minim", "nihil", "consequuntur", "eum", "consequuntur", "error", "qui", "et",
                    "eos", "minim", "numquam", "commodo", "architecto", "ut", "Cicero", "deserunt", "Finibus", "sunt", "nesciunt.", "molestiae", "Quis",
                    "THIS_IS_UNIQUE_VALUE,", "eum", "in"
                },
            };
            var entry1 = new IndexEntry
            {
                Id = "entry/1",
                Content = new string[]
                {
                    "incididunt", "fugiat", "quia", "consequatur?", "magnam", "officia", "elit,", "illum", "ipsa", "of", "culpa", "ea", "voluptas", "Duis",
                    "voluptatem", "Lorem", "modi", "qui", "Sed", "veritatis", "written", "ea", "mollit", "sint", "porro", "ratione", "THIS_IS_UNIQUE_VALUE,",
                    "consectetur", "laudantium,", "aliquam"
                },
            };
            var entry2 = new IndexEntry
            {
                Id = "entry/2",
                Content = new string[]
                {
                    "laboris", "natus", "Neque", "consequatur,", "qui", "ut", "natus", "illo", "Quis", "voluptas", "eaque", "quasi", "", "aut", "esse", "sed",
                    "qui", "aut", "eos", "eius", "quia", "esse", "aliquip", "", "vel", "quia", "aliqua.", "quia", "consequatur,", "Sed"
                },
            };
            var entry3 = new IndexEntry
            {
                Id = "entry/3",
                Content = new string[]
                {
                    "enim", "aliquid", "voluptas", "Finibus", "eaque", "esse", "Duis", "aut", "voluptatem.", "reprehenderit", "ad", "illum", "consequatur?",
                    "architecto", "velit", "esse", "veniam,", "amet,", "voluptatem", "accusantium", "THIS_IS_UNIQUE_VALUE.", "dolore", "eum", "laborum.", "ipsam",
                    "of", "explicabo.", "voluptatem", "et", "quis"
                },
            };
            var entry4 = new IndexEntry
            {
                Id = "entry/4",
                Content = new string[]
                {
                    "incididunt", "id", "ratione", "inventore", "pariatur.", "molestiae", "dolor", "sit", "Nemo", "de", "nulla", "et", "proident,", "quae",
                    "ipsam", "iste", "in", "dolore", "culpa", "enim", "dolor", "consectetur", "veritatis", "of", "45", "fugiat", "magnam", "Bonorum", "dolor",
                    "beatae"
                },
            };
            var entry5 = new IndexEntry
            {
                Id = "entry/5",
                Content = new string[]
                {
                    "laboriosam,", "totam", "voluptate", "et", "sit", "culpa", "reprehenderit", "eius", "accusantium", "", "omnis", "beatae", "amet,", "nulla",
                    "tempor", "ullamco", "dolor", "ipsam", "vel", "THIS_IS_UNIQUE_VALUE", "quia", "", "consequatur,", "labore", "aliqua.", "dicta", "nostrum",
                    "ut", "dolorem", "Duis"
                },
            };
            var entry6 = new IndexEntry
            {
                Id = "entry/6",
                Content = new string[]
                {
                    "enim", "sed", "ad", "deserunt", "eu", "omnis", "voluptate", "in", "qui", "rem", "sunt", "tempor", "voluptatem", "vel", "enim", "velit",
                    "velit", "aliquip", "by", "in", "eum", "dolore", "incidunt", "commodi", "anim", "amet,", "quo", "est,", "ratione", "sit"
                },
            };
            var entry7 = new IndexEntry
            {
                Id = "entry/7",
                Content = new string[]
                {
                    "sed", "qui", "esse", "THIS_IS_UNIQUE_VALUE", "dolore", "totam", "Nemo", "veniam,", "reprehenderit", "consequuntur", "consequuntur",
                    "aperiam,", "fugiat", "sed", "corporis", "45", "culpa", "accusantium", "quae", "dolor", "voluptate", "dolor", "et", "explicabo.", "voluptate",
                    "Nemo", "tempora", "accusantium", "dolore", "in"
                },
            };
            var entry8 = new IndexEntry
            {
                Id = "entry/8",
                Content = new string[]
                {
                    "nihil", "velit", "quia", "amet,", "fugit,", "eiusmod", "magna", "aliqua.", "ullamco", "accusantium", "nulla", "ex", "sit", "quo", "sit",
                    "sit", "enim", "qui", "sunt", "aspernatur", "laboris", "autem", "voluptas", "amet,", "ipsa", "commodo", "minima", "consectetur,", "fugiat",
                    "voluptas"
                },
            };
            var entry9 = new IndexEntry
            {
                Id = "entry/9",
                Content = new string[]
                {
                    "dolorem", "ipsa", "in", "omnis", "ullamco", "ab", "esse", "aut", "rem", "eu", "iure", "ad", "consequuntur", "est", "adipisci", "velit",
                    "inventore", "nesciunt.", "ad", "vitae", "laborum.", "esse", "voluptate", "et", "fugiat", "fugiat", "voluptas", "quae", "dolor", "qui"
                },
            };
            var entries = new[] {entry0, entry1, entry2, entry3, entry4, entry5, entry6, entry7, entry8, entry9};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entries, CreateKnownFields(bsc));
            
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var contentMetadata = searcher.FieldMetadataBuilder("Content", ContentIndex);

            {
                var match = searcher.AllInQuery(contentMetadata, new HashSet<(string Term, bool Exact)>() {("quo", false), ("in", false)});

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.AllInQuery(contentMetadata, new HashSet<string>()
                {
                    "dolorem",
                    "ipsa",
                    "in",
                    "omnis",
                    "ullamco",
                    "ab",
                    "esse",
                    "aut",
                    "rem",
                    "eu",
                    "iure",
                    "ad",
                    "consequuntur",
                    "est",
                    "adipisci",
                    "velit",
                    "inventore",
                    "nesciunt.",
                    "ad",
                    "vitae",
                    "laborum.",
                    "esse",
                    "voluptate",
                    "et",
                    "fugiat",
                    "fugiat",
                    "voluptas",
                    "quae",
                    "dolor",
                    "qui"
                }.Select(x => (x, false)).ToHashSet());

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.AllInQuery(contentMetadata, new HashSet<string>()
                {
                    "dolorem",
                    "ipsa",
                    "in",
                    "omnis",
                    "ullamco",
                    "ab",
                    "esse",
                    "aut",
                    "rem",
                    "eu",
                    "iure",
                    "ad",
                    "consequuntur",
                    "est",
                    "adipisci",
                    "velit",
                    "inventore",
                    "nesciunt.",
                    "ad",
                    "vitae",
                    "laborum.",
                    "esse",
                    "voluptate",
                    "et",
                    "fugiat",
                    "fugiat",
                    "voluptas",
                    "quae",
                    "dolor",
                    "THIS_IS_SUPER_UNIQUE_VALUE"
                }.Select(x => (x, false)).ToHashSet());

                Span<long> ids = stackalloc long[16];
                Assert.Equal(0, match.Fill(ids));
            }
        }


        [Fact]
        public void SimpleStartWithStatement()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"a road", "a lake", "the mountain"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"a road", "the mountain"},};
            var entry3 = new IndexEntry {Id = "entry/3", Content = new string[] {"the sky", "the space", "an animal"},};


            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            {
                var match = searcher.StartWithQuery("Content", "a");

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.StartWithQuery("Content", "the s");

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.StartWithQuery("Content", "an");

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.StartWithQuery("Content", "a");

                Span<long> ids = stackalloc long[2];

                int idCount = match.Fill(ids);
                Assert.NotEqual(0, idCount);
                idCount += match.Fill(ids);
                Assert.NotEqual(0, idCount);
                Assert.Equal(0, match.Fill(ids));

                Assert.Equal(3, idCount);
            }
        }

        [Fact]
        public void MixedSortedMatchStatement()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "3"};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"4", "2"},};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "1"};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry3}, CreateKnownFields(bsc));
            IndexEntries(bsc, new[] {entry2}, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var contentMetadata = searcher.FieldMetadataBuilder("Content", ContentIndex);
            OrderMetadata orderMetadata = new OrderMetadata(contentMetadata, true, MatchCompareFieldType.Sequence);
            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.OrderBy(match1, orderMetadata, take: 16);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }
        }


        [Fact]
        public void WillGetTotalNumberOfResultsInPagedQuery()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "3"};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"4", "2"},};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "1"};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry3}, CreateKnownFields(bsc));
            IndexEntries(bsc, new[] {entry2}, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var contentMetadata = searcher.FieldMetadataBuilder("Content", ContentIndex);
            OrderMetadata orderMetadata = new OrderMetadata(contentMetadata, true, MatchCompareFieldType.Sequence);
            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.OrderBy(match1, orderMetadata);

                Span<long> ids = stackalloc long[2];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));

                Assert.Equal(3, match.TotalResults);
            }
        }

        [Fact]
        public void CanGetAllEntries()
        {
            var list = new List<IndexSingleEntry>();
            int i;
            for (i = 0; i < 1024; ++i)
            {
                list.Add(new IndexSingleEntry() {Id = $"entry/{i + 1}", Content = i.ToString()});
            }

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, list, CreateKnownFields(bsc));
            IndexEntries(bsc, new[] {new IndexEntry() {Id = $"entry/{i + 1}"}}, CreateKnownFields(bsc));

            list.Add(new IndexSingleEntry() {Id = $"entry/{i + 1}"});

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            {
                var all = searcher.AllEntries();
                var results = new List<string>();
                int read;
                Span<long> ids = stackalloc long[256];
                while ((read = all.Fill(ids)) != 0)
                {
                    for (i = 0; i < read; ++i)
                    {
                        long id = ids[i];
                        results.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
                    }
                }

                results.Sort();
                list.Sort((x, y) => x.Id.CompareTo(y.Id));
                Assert.Equal(list.Count, results.Count);
                for (i = 0; i < all.Count; ++i)
                    Assert.Equal(list[i].Id, results[i]);
            }
        }

        [Fact]
        public void SimpleSortedMatchStatement()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "3"};
            var entry2 = new IndexSingleEntry {Id = "entry/2", Content = "2"};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "1"};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(bsc));
            var contentMetadata = searcher.FieldMetadataBuilder("Content", ContentIndex);
            OrderMetadata orderMetadata = new OrderMetadata(contentMetadata, true, MatchCompareFieldType.Sequence);
            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.OrderBy(match1, orderMetadata);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));

                long id = ids[0];
                Assert.Equal("entry/3", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
                long id1 = ids[1];
                Assert.Equal("entry/2", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id1));
                long id2 = ids[2];
                Assert.Equal("entry/1", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id2));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.OrderBy(match1, orderMetadata, take: 16);

                Span<long> ids1 = stackalloc long[2];
                Assert.Equal(2, match.Fill(ids1));
                long id = ids1[0];
                Assert.Equal("entry/3", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
                long id1 = ids1[1];
                Assert.Equal("entry/2", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id1));
                
                Span<long> ids2 = stackalloc long[2];
                Assert.Equal(1, match.Fill(ids2));
                long id2 = ids2[0];
                Assert.Equal("entry/1", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id2));

                Assert.Equal(0, match.Fill(ids2));
            }
        }

        [Fact]
        public void SimpleOrdinalCompareStatementWithLongValue()
        {
            var list = new List<IndexSingleEntryDouble>();
            for (int i = 1; i < 1001; ++i)
                list.Add(new IndexSingleEntryDouble() {Id = $"entry/{i}", Content = (double)i});
            List<string> qids = new();
            IndexEntriesDouble(list);
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            Span<long> ids = stackalloc long[1024];
            var contentMetadata = searcher.FieldMetadataBuilder("Content", ContentIndex);
            {
                var match1 = searcher.AllEntries();
                var match2 = searcher.CreateMultiUnaryMatch(searcher.AllEntries(), [new MultiUnaryItem(contentMetadata, 25D, UnaryMatchOperation.LessThan)]);

                int read = 0;
                while ((read = match2.Fill(ids)) != 0)
                while (--read >= 0)
                {
                    long id = ids[read];
                    qids.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
                }

                foreach (IndexSingleEntryDouble indexSingleEntryDouble in list)
                {
                    bool isIn = qids.Contains(indexSingleEntryDouble.Id);
                    if (indexSingleEntryDouble.Content >= 25D)
                        Assert.False(isIn);
                    else
                        Assert.True(isIn);
                }
            }

            qids.Clear();
            {
                var matchBetween = searcher.CreateMultiUnaryMatch(searcher.AllEntries(),
                    [new(contentMetadata, 100L, 200L, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual)]);
                var read = matchBetween.Fill(ids);
                while (--read >= 0)
                {
                    long id = ids[read];
                    qids.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
                }

                foreach (IndexSingleEntryDouble indexSingleEntryDouble in list)
                {
                    bool isIn = qids.Contains(indexSingleEntryDouble.Id);
                    if (indexSingleEntryDouble.Content is >= 100L and <= 200L)
                        Assert.True(isIn);
                    else
                        Assert.False(isIn);
                }
            }
        }


        [Fact]
        public void SimpleOrdinalCompareStatement()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "3"};
            var entry2 = new IndexSingleEntry {Id = "entry/2", Content = "2"};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "1"};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            Slice.From(bsc, "1", out var one);
            var contentMetadata = searcher.FieldMetadataBuilder("Content", ContentIndex);
            {
                //   var match1 = searcher.StartWithQuery("Id", "e");
                var match1 = searcher.AllEntries();
                var match = searcher.CreateMultiUnaryMatch(match1,
                    [new MultiUnaryItem(searcher, contentMetadata, "1", UnaryMatchOperation.GreaterThan)]);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.CreateMultiUnaryMatch(match1,
                    [new MultiUnaryItem(searcher, contentMetadata, "1", UnaryMatchOperation.GreaterThanOrEqual)]);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.CreateMultiUnaryMatch(match1,
                    [new MultiUnaryItem(searcher, contentMetadata, "1", UnaryMatchOperation.LessThan)]);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.CreateMultiUnaryMatch(match1,
                    [new MultiUnaryItem(searcher, contentMetadata, "1", UnaryMatchOperation.LessThanOrEqual)]);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }
        }


        [Fact]
        public void SimpleEqualityCompareStatement()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "1"};
            var entry2 = new IndexSingleEntry {Id = "entry/2", Content = "2"};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "1"};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var contentMetadata = searcher.FieldMetadataBuilder("Content", ContentIndex);

            Slice.From(bsc, "1", out var one);
            Slice.From(bsc, "4", out var four);

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.CreateMultiUnaryMatch(match1,
                    [new MultiUnaryItem(searcher, contentMetadata, "1", UnaryMatchOperation.Equals)]);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.CreateMultiUnaryMatch(match1,
                    [new MultiUnaryItem(searcher, contentMetadata, "1", UnaryMatchOperation.NotEquals)]);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.CreateMultiUnaryMatch(match1,
                    [new MultiUnaryItem(searcher, contentMetadata, "4", UnaryMatchOperation.Equals)]);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.CreateMultiUnaryMatch(match1,
                    [new MultiUnaryItem(searcher, contentMetadata, "4", UnaryMatchOperation.NotEquals)]);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }
        }

        [Fact]
        public void SimpleWildcardStatement()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "Testing"};
            var entry2 = new IndexSingleEntry {Id = "entry/2", Content = "Running"};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "Runner"};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var contentMetadata = searcher.FieldMetadataBuilder("Content", ContentIndex);
            using var _ = Slice.From(bsc, "ing", out var ingSlice);

            Slice.From(bsc, "1", out var one);
            Slice.From(bsc, "4", out var four);

            {
                var match = searcher.ContainsQuery(contentMetadata, ingSlice);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.ContainsQuery(contentMetadata, "er");
                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                long id = ids[0];
                Assert.Equal("entry/3", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
            }

            {
                var match = searcher.StartWithQuery(contentMetadata, "Run", true);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                long id = ids[0];
                Assert.Equal("entry/1", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
            }

            {
                var match = searcher.EndsWithQuery(contentMetadata, "ing", false);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
                long id = ids[0];
                long id1 = ids[1];
                var results = new[] {searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id), searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id1)};
                Array.Sort(results);
                Assert.Equal("entry/1", results[0]);
                Assert.Equal("entry/2", results[1]);
            }

            {
                var match = searcher.EndsWithQuery(contentMetadata, "ing", true);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                long id = ids[0];
                Assert.Equal("entry/3", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
            }

            {
                var match = searcher.ContainsQuery(contentMetadata, "Run");

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.ContainsQuery(contentMetadata, "nn");

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.ContainsQuery(contentMetadata, "run");

                Span<long> ids = stackalloc long[16];
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.EndsWithQuery(contentMetadata, "ing");
                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
            }
        }

        [Fact]
        public void SimpleBetweenCompareStatement()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "3"};
            var entry2 = new IndexSingleEntry {Id = "entry/2", Content = "2"};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "1"};
            var entry4 = new IndexSingleEntry {Id = "entry/4", Content = "4"};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3, entry4}, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var contentMetadata = searcher.FieldMetadataBuilder("Content", ContentIndex);
            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.CreateMultiUnaryMatch(match1,
                    [new MultiUnaryItem(searcher, contentMetadata, "1", "2", UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual)]);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.CreateMultiUnaryMatch(match1,
                    [new MultiUnaryItem(searcher, contentMetadata, "0", "3", UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual)]);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.CreateMultiUnaryMatch(match1,
                    [new MultiUnaryItem(searcher, contentMetadata, "0", "0", UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual)]);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.CreateMultiUnaryMatch(match1,
                    [new MultiUnaryItem(searcher, contentMetadata, "1", "1", UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual)]);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }
        }

        [Fact]
        public void BetweenWithCustomComparers()
        {
            var entries = Enumerable.Range(0, 100).Select(i => new IndexSingleEntryDouble() {Id = $"entry{i}", Content = Convert.ToDouble(i)}).ToList();
            IndexEntriesDouble(entries);
            int? read;
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var contentMetadata = searcher.FieldMetadataBuilder("Content", ContentIndex);

            {
                Span<long> ids = stackalloc long[128];
                var match = searcher.CreateMultiUnaryMatch(searcher.AllEntries(),
                    [new MultiUnaryItem(contentMetadata, 20L, 30L, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual)]);

                Assert.Equal(entries.Count(i => i.Content is >= 20 and <= 30), read = match.Fill(ids));
                for (int i = 0; i < read; ++i)
                    Check(ids[i], 20, 30, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual);
            }

            {
                Span<long> ids = stackalloc long[128];
                var match = searcher.CreateMultiUnaryMatch(searcher.AllEntries(),
                    [new MultiUnaryItem(contentMetadata, 20L, 30L, UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual)]);
                Assert.Equal(entries.Count(i => i.Content is > 20 and <= 30), read = match.Fill(ids));
                for (int i = 0; i < read; ++i)
                    Check(ids[i], 20, 30, UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual);
            }

            {
                Span<long> ids = stackalloc long[128];
                var match = searcher.CreateMultiUnaryMatch(searcher.AllEntries(),
                    [new MultiUnaryItem(contentMetadata, 20L, 30L, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan)]);
                Assert.Equal(entries.Count(i => i.Content is >= 20 and < 30), read = match.Fill(ids));
                for (int i = 0; i < read; ++i)
                    Check(ids[i], 20, 30, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan);
            }

            {
                Span<long> ids = stackalloc long[128];
                var match = searcher.CreateMultiUnaryMatch(searcher.AllEntries(),
                    [new MultiUnaryItem(contentMetadata, 20L, 30L, UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan)]);
                Assert.Equal(entries.Count(i => i.Content is > 20 and < 30), read = match.Fill(ids));
                for (int i = 0; i < read; ++i)
                    Check(ids[i], 20, 30, UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan);
            }

            void Check(long id, long left, long right, UnaryMatchOperation leftComparer, UnaryMatchOperation rightComparer)
            {
                var entry = searcher.TermsReaderFor("Content").GetTermFor(id);
                var number = long.Parse(entry);
                Assert.True(PerformUnaryMatch(number));


                bool PerformUnaryMatch(long value) =>
                    (leftComparer, rightComparer) switch
                    {
                        (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => value >= left && right >= value,
                        (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => value > left && right >= value,
                        (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => value >= left && right > value,
                        (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => value > left && right > value,
                        _ => throw new NotSupportedException()
                    };
            }
        }
        
        [Theory]
        [InlineData(new object[] {1000, 8})]
        public void AndInStatementWithLowercaseAnalyzer(int setSize, int stackSize)
        {
            setSize = setSize - (setSize % 3);
            var entries = new List<IndexEntry>();
            var entriesToIndex = new IndexEntry[setSize];
            for (int i = 0; i < setSize; i++)
            {
                var entry = new IndexEntry
                {
                    Id = $"entry/{i}",
                    Content = (i % 3) switch
                    {
                        0 => ["road", "Lake", "mounTain"],
                        1 => ["roAd", "mountain"],
                        2 => ["sky", "space", "laKe"],
                        _ => throw new InvalidDataException("This should not happen.")
                    }
                };
                entries.Add(entry);
                entriesToIndex[i] = entry;
            }

            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            var analyzer = Analyzer.Create<KeywordTokenizer, LowerCaseTransformer>(ctx);

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entriesToIndex, CreateKnownFields(bsc, analyzer));


            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            {
                var match1 = searcher.InQuery("Content", new() {"lake", "mountain"});
                var match2 = searcher.TermQuery("Content", "sky");
                var andMatch = searcher.And(in match1, in match2);

                var actual = new List<long>();
                Span<long> ids = stackalloc long[stackSize];
                int read;
                do
                {
                    read = andMatch.Fill(ids);
                    actual.AddRange(ids[..read].ToArray());
                } while (read != 0);

                var actualSorted = actual.ToArray();
                var actualSize = Sorting.SortAndRemoveDuplicates(actualSorted.AsSpan());

                Assert.Equal((setSize / 3), actualSize);
            }

            {
                var match1 = searcher.TermQuery("Content", "sky");
                var match2 = searcher.InQuery("Content", new() {"lake", "mountain"});
                var andMatch = searcher.And(in match1, in match2);


                var actual = new List<long>();
                Span<long> ids = stackalloc long[stackSize];
                int read;
                do
                {
                    read = andMatch.Fill(ids);
                    actual.AddRange(ids[..read].ToArray());
                } while (read != 0);

                var actualSorted = actual.ToArray();
                var actualSize = Sorting.SortAndRemoveDuplicates(actualSorted.AsSpan());

                Assert.Equal((setSize / 3), actualSize);
            }
        }

        [Theory]
        [InlineData(new object[] {1000, 8})]
        public void AndInStatementAndWhitespaceTokenizer(int setSize, int stackSize)
        {
            setSize = setSize - (setSize % 3);

            var entriesToIndex = new IndexEntry[setSize];
            for (int i = 0; i < setSize; i++)
            {
                var entry = new IndexEntry
                {
                    Id = $"entry/{i}",
                    Content = (i % 3) switch
                    {
                        0 => new string[] {"road Lake mounTain  "},
                        1 => new string[] {"roAd mountain"},
                        2 => new string[] {"sky space laKe"},
                        _ => throw new InvalidDataException("This should not happen.")
                    }
                };

                entriesToIndex[i] = entry;
            }

            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            var analyzer = Analyzer.Create<WhitespaceTokenizer, LowerCaseTransformer>(ctx);
            using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
                .AddBinding(IdIndex, idSlice, analyzer)
                .AddBinding(ContentIndex, contentSlice, analyzer);
            using var mapping = builder.Build();

            IndexEntries(ctx, entriesToIndex, mapping);

            using var searcher = new IndexSearcher(Env, mapping);
            {
                var match1 = searcher.InQuery("Content", new() {"lake", "mountain"});
                var match2 = searcher.TermQuery("Content", "sky");
                var andMatch = searcher.And(in match1, in match2);

                var actual = new List<long>();
                Span<long> ids = stackalloc long[stackSize];
                int read;
                do
                {
                    read = andMatch.Fill(ids);
                    actual.AddRange(ids[..read].ToArray());
                } while (read != 0);

                var actualSorted = actual.ToArray();
                var actualSize = Sorting.SortAndRemoveDuplicates(actualSorted.AsSpan());

                Assert.Equal((setSize / 3), actualSize);
            }

            {
                var match1 = searcher.TermQuery("Content", "sky");
                var match2 = searcher.InQuery("Content", new() {"lake", "mountain"});
                var andMatch = searcher.And(in match1, in match2);

                var actual = new List<long>();
                Span<long> ids = stackalloc long[stackSize];
                int read;
                do
                {
                    read = andMatch.Fill(ids);
                    actual.AddRange(ids[..read].ToArray());
                } while (read != 0);

                var actualSorted = actual.ToArray();
                var actualSize = Sorting.SortAndRemoveDuplicates(actualSorted.AsSpan());

                Assert.Equal((setSize / 3), actualSize);
            }
        }

        [Fact]
        public void StartsWithSingle()
        {
            var entry = new IndexSingleEntry {Id = $"entry/1", Content = "tester"};
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            var analyzer = Analyzer.Create<WhitespaceTokenizer, LowerCaseTransformer>(ctx);
            using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
                .AddBinding(IdIndex, idSlice, analyzer)
                .AddBinding(ContentIndex, contentSlice, analyzer);
            using var mapping = builder.Build();

            IndexEntries(ctx, new[] {entry}, mapping);
            using (var searcher = new IndexSearcher(Env, mapping))
            {
                var match = searcher.StartWithQuery("Content", "test");
                var ids = new long[16];
                var matchEq = searcher.TermQuery("Content", "tester");
                Assert.Equal(1, matchEq.Fill(ids));
                Assert.Equal(1, match.Fill(ids));
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void NotInTest()
        {
            var listToIndex = Enumerable.Range(000000, 1000).Select(i => new IndexSingleEntry {Id = $"entry/{i}", Content = i.ToString("000000")}).ToList();
            var listForNotIn = listToIndex.Where(p => p.Content.EndsWith("1")).ToList();
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, listToIndex, CreateKnownFields(bsc));
            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            {
                Span<long> ids = stackalloc long[1024];
                var match = searcher.AndNot(searcher.AllEntries(), searcher.InQuery(searcher.FieldMetadataBuilder("Content", ContentIndex), listForNotIn.Select(l => l.Content).ToList()));
                Assert.Equal(1000 - listForNotIn.Count(), match.Fill(ids));
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleAndNot()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "Testing"};
            var entry2 = new IndexSingleEntry {Id = "entry/2", Content = "Running"};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "Runner"};
            var list = new[] {entry1, entry2, entry3};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, list, CreateKnownFields(bsc));


            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            {
                var andNotMatch = searcher.AndNot(searcher.AllEntries(), searcher.StartWithQuery("Content", "Run"));

                Span<long> ids = stackalloc long[256];
                Assert.Equal(1, andNotMatch.Fill(ids));
                var item = searcher.TermsReaderFor("Id").GetTermFor(ids[0]);
                Assert.Equal("entry/1", item);
            }

            {
                var andNotMatch = searcher.AndNot(searcher.AllEntries(), searcher.AllEntries());

                Span<long> ids = stackalloc long[256];
                Assert.Equal(0, andNotMatch.Fill(ids));
            }

            {
                var andNotMatch = searcher.AndNot(searcher.AllEntries(), searcher.StartWithQuery("Content", "J"));

                Span<long> ids = stackalloc long[256];
                Assert.Equal(3, andNotMatch.Fill(ids));
                var uniqueList = new List<long>();
                for (int i = 0; i < 3; ++i)
                {
                    Assert.False(uniqueList.Contains(ids[i]));
                    uniqueList.Add(ids[i]);
                }
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void NotEqualWithList()
        {
            var entries = new List<IndexEntry>();
            var entriesToIndex = new IndexEntry[7];
            for (int i = 0; i < 7; i++)
            {
                var entry = new IndexEntry
                {
                    Id = $"entry/{i}",
                    Content = (i % 7) switch
                    {
                        0 => ["1"],
                        1 => ["7"],
                        2 => ["1", "2"],
                        3 => ["1", "2", "3"],
                        4 => ["1", "2", "3", "5"],
                        5 => ["2", "5"],
                        6 => ["2", "5", "7"],
                        _ => throw new ArgumentOutOfRangeException()
                    }
                };
                entries.Add(entry);
                entriesToIndex[i] = entry;
            }

            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            //":{"p0":"8 9 10"}}
            IndexEntries(bsc, entriesToIndex, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var contentMetadata = searcher.FieldMetadataBuilder("Content", ContentIndex);
            {
                var match0 = searcher.CreateMultiUnaryMatch(searcher.AllEntries(), [new MultiUnaryItem(searcher, contentMetadata, "8", UnaryMatchOperation.NotEquals)]);
                var match1 = searcher.CreateMultiUnaryMatch(searcher.AllEntries(), [new MultiUnaryItem(searcher, contentMetadata, "9", UnaryMatchOperation.NotEquals)]);
                var match2 = searcher.CreateMultiUnaryMatch(searcher.AllEntries(), [new MultiUnaryItem(searcher, contentMetadata, "10", UnaryMatchOperation.NotEquals)]);
                var firstOr = searcher.Or(match0, match1);
                var finalOr = searcher.And(searcher.StartWithQuery("Id", "e"), searcher.Or(firstOr, match2));


                Span<long> ids = stackalloc long[256];
                Assert.Equal(7, finalOr.Fill(ids));
            }

            {
                var m0 = searcher.CreateMultiUnaryMatch(searcher.AllEntries(), [new MultiUnaryItem(searcher, contentMetadata, "1", UnaryMatchOperation.NotEquals)]);
                var m1 = searcher.CreateMultiUnaryMatch(searcher.AllEntries(), [new MultiUnaryItem(searcher, contentMetadata, "2", UnaryMatchOperation.NotEquals)]);
                var m2 = searcher.CreateMultiUnaryMatch(searcher.AllEntries(), [new MultiUnaryItem(searcher, contentMetadata, "3", UnaryMatchOperation.NotEquals)]);
                var m3 = searcher.CreateMultiUnaryMatch(searcher.AllEntries(), [new MultiUnaryItem(searcher, contentMetadata, "5", UnaryMatchOperation.NotEquals)]);
                var m4 = searcher.CreateMultiUnaryMatch(searcher.AllEntries(), [new MultiUnaryItem(searcher, contentMetadata, "7", UnaryMatchOperation.NotEquals)]);

                Span<long> ids = stackalloc long[256];
                var orResult = searcher.Or(m4, searcher.Or(m3, searcher.Or(m2, searcher.Or(m1, m0))));
                Assert.Equal(7, orResult.Fill(ids));
                Assert.True(ids.Slice(0, 7).ToArray().ToList().Select(x => searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(x)).OrderBy(a => a)
                    .SequenceEqual(entries.OrderBy(z => z.Id).Select(e => e.Id)));
            }

            {
                Span<long> ids = stackalloc long[256];
                var startsWith = searcher.StartWithQuery("Id", "e");
                Assert.Equal(7, startsWith.Fill(ids));

                Assert.True(ids.Slice(0, 7).ToArray().ToList().Select(x => searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(x)).OrderBy(a => a)
                    .SequenceEqual(entries.OrderBy(z => z.Id).Select(e => e.Id)));
            }

            {
                var m0 = searcher.CreateMultiUnaryMatch(searcher.AllEntries(), [new MultiUnaryItem(searcher, contentMetadata, "1", UnaryMatchOperation.NotEquals)]);
                var m1 = searcher.CreateMultiUnaryMatch(searcher.AllEntries(), [new MultiUnaryItem(searcher, contentMetadata, "2", UnaryMatchOperation.NotEquals)]);
                var m2 = searcher.CreateMultiUnaryMatch(searcher.AllEntries(), [new MultiUnaryItem(searcher, contentMetadata, "3", UnaryMatchOperation.NotEquals)]);
                var m3 = searcher.CreateMultiUnaryMatch(searcher.AllEntries(), [new MultiUnaryItem(searcher, contentMetadata, "5", UnaryMatchOperation.NotEquals)]);
                var m4 = searcher.CreateMultiUnaryMatch(searcher.AllEntries(), [new MultiUnaryItem(searcher, contentMetadata, "7", UnaryMatchOperation.NotEquals)]);
                
                var result = searcher.And(searcher.StartWithQuery("Id", "e"), searcher.Or(m4, searcher.Or(m3, searcher.Or(m2, searcher.Or(m1, m0)))));

                Span<long> ids = stackalloc long[256];
                var amount = result.Fill(ids.Slice(14));
                var idsOfResult = ids.Slice(14, amount).ToArray().Select(x => searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(x)).ToList();
                Assert.Equal(idsOfResult.Count, idsOfResult.Distinct().Count());
                Assert.Equal(7, amount);
                Assert.True(idsOfResult.SequenceEqual(entries.OrderBy(z => z.Id).Select(e => e.Id)));
            }
        }

        [Theory]
        [InlineData(100, 16)]
        [InlineData(1000, 128)]
        [InlineData(10_000, 128)]
        [InlineData(10_000, 256)]
        [InlineData(10_000, 512)]
        [InlineData(10_000, 1028)]
        public void MultiTermMatchWithBinaryOperations(int setSize, int stackSize)
        {
            var words = new[]
            {
                "torun", "pomorze", "maciej", "aszyk", "corax", "matt", "gracjan", "tomasz", "marcin", "tomtom", "ravendb", "poland", "israel", "pattern", "seen",
                "macios", "tests", "are", "cool", "arent", "they", "this", "should", "work", "every", "time"
            };
            var random = new Random(1000);
            var entries = Enumerable.Range(0, setSize).Select(i => new IndexEntry() {Id = $"entry/{i}", Content = GetContent()}).ToList();

            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entries.ToArray(), CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            {
                //MultiTermMatch And TermMatch
                var match0 = searcher.InQuery("Content", new List<string>() {"maciej", "poland"});
                var match1 = searcher.TermQuery("Content", "this");
                var and = searcher.And(match0, match1);
                var result = Act(and);
                var resultByLinq = entries.Where(x => (x.Content.Contains("maciej") || x.Content.Contains("poland")) && x.Content.Contains("this")).ToList();
                Assert.Equal(result.Count, result.Distinct().Count());
                Assert.Equal(resultByLinq.Count, result.Count);
            }

            {
                var match0 = searcher.StartWithQuery("Content", "ma");
                var match1 = searcher.TermQuery("Content", "torun");

                var matchOr = searcher.Or(match0, match1);
                var result = Act(matchOr);
                var linqResult = entries.Where(x => x.Content.Any(z => z.StartsWith("ma") || z.Contains("torun"))).ToList();
                Assert.Equal(linqResult.Count, result.Count);
            }

            List<string> Act(IQueryMatch query)
            {
                List<string> stringIds = new();
                int read;
                Span<long> ids = stackalloc long[stackSize];
                while ((read = query.Fill(ids)) != 0)
                {
                    for (int i = 0; i < read; ++i)
                    {
                        long id = ids[i];
                        stringIds.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
                    }
                }

                return stringIds.Distinct().ToList();
            }

            string[] GetContent()
            {
                var amount = random.Next(0, 10);
                return Enumerable.Range(0, amount).Select(i => words[random.Next(0, words.Count())]).ToArray();
            }
        }

        [Fact]
        public void UnaryMatch()
        {
            var entries = new List<IndexEntry>();
            var entriesToIndex = new IndexEntry[7];
            for (int i = 0; i < 7; i++)
            {
                var entry = new IndexEntry
                {
                    Id = $"entry/{i}",
                    Content = (i % 7) switch
                    {
                        0 => new string[] {"1"},
                        1 => new string[] {null, "7"},
                        2 => new string[] {"2", "1"},
                        3 => new string[] {null, "1", "2", "3"},
                        4 => new string[] {"1", "2", "3", "5", null},
                        5 => new string[] {"2", "5"},
                        6 => new string[] {"2", "5", "7"},
                        _ => throw new ArgumentOutOfRangeException()
                    }
                };
                entries.Add(entry);
                entriesToIndex[i] = entry;
            }

            IndexEntries(Allocator, entries.ToArray(), CreateKnownFields(Allocator));

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var contentMetadata = searcher.FieldMetadataBuilder("Content", ContentIndex);
            {
                var notOne = searcher.CreateMultiUnaryMatch(searcher.AllEntries(), 
                    new []{new MultiUnaryItem(searcher, contentMetadata, "1", UnaryMatchOperation.NotEquals)});
                Span<long> ids = stackalloc long[32];
                var expected = entries.Count(x => x.Content.Contains("1") == false);
                var result = notOne.Fill(ids);
                List<string> xd = new();
                foreach (var id in ids.Slice(0, result))
                {
                    xd.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
                }

                Assert.Equal(3, result);
            }
            {
                var notTwo = searcher.CreateMultiUnaryMatch(searcher.AllEntries(), 
                    new []{new MultiUnaryItem(searcher, contentMetadata, "2", UnaryMatchOperation.NotEquals)});
                Span<long> ids = stackalloc long[32];
                var expected = entries.Count(x => x.Content.Contains("2") == false);
                var result = notTwo.Fill(ids);
                List<string> xd = new();
                foreach (var id in ids.Slice(0, result))
                {
                    xd.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
                }


                Assert.Equal(expected, result);
            }
        }
        
        
        private class IndexEntry
        {
            public long IndexEntryId;
            public string Id;
            public string[] Content;
        }

        private class IndexSingleEntry
        {
            public string Id;
            public string Content;
        }

        private readonly struct StringArrayIterator : IReadOnlySpanIndexer
        {
            private readonly string[] _values;

            private static string[] Empty = new string[0];

            public StringArrayIterator(string[] values)
            {
                _values = values ?? Empty;
            }

            public StringArrayIterator(IEnumerable<string> values)
            {
                _values = values?.ToArray() ?? Empty;
            }

            public int Length => _values.Length;

            public bool IsNull(int i)
            {
                if (i < 0 || i >= Length)
                    throw new ArgumentOutOfRangeException();

                return _values[i] == null;
            }

            public ReadOnlySpan<byte> this[int i] => _values[i] != null ? Encoding.UTF8.GetBytes(_values[i]) : null;
        }

  
        public const int IdIndex = 0,
            ContentIndex = 1;

        private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx, Analyzer analyzer = null)
        {
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
                .AddBinding(IdIndex, idSlice, analyzer)
                .AddBinding(ContentIndex, contentSlice, analyzer);
            return builder.Build();
        }

        private void IndexEntries(ByteStringContext bsc, IEnumerable<IndexEntry> list, IndexFieldsMapping mapping)
        {
            using var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All);

            foreach (var entry in list)
            {
                using var builder = indexWriter.Index(entry.Id);
                builder.Write(IdIndex, PrepareString(entry.Id));
                if (entry.Content != null)
                {
                    foreach (string s in entry.Content)
                    {
                        if (s == null)
                        {
                            builder.WriteNull(ContentIndex, null);
                        }
                        else
                        {
                            builder.Write(ContentIndex, Encoding.UTF8.GetBytes(s));
                        }
                    }
                }

                entry.IndexEntryId = builder.EntryId;
            }
            indexWriter.Commit();
            mapping.Dispose();
        }

        private void IndexEntries(ByteStringContext bsc, IEnumerable<IndexSingleEntry> list, IndexFieldsMapping mapping)
        {
            using var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All);

            foreach (var entry in list)
            {
                using var builder = indexWriter.Index(entry.Id);
                builder.Write(IdIndex, PrepareString(entry.Id));
                builder.Write(ContentIndex, PrepareString(entry.Content));

            }

            indexWriter.Commit();
        }

        private void IndexEntriesDouble(IEnumerable<IndexSingleEntryDouble> list)
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            var knownFields = CreateKnownFields(bsc);

            {
                using var indexWriter = new IndexWriter(Env, knownFields, SupportedFeatures.All);

                foreach (var entry in list)
                {
                    using var entryWriter = indexWriter.Index(entry.Id);
                    entryWriter.Write(IdIndex, PrepareString(entry.Id));
                    entryWriter.Write(ContentIndex, PrepareString(entry.Content.ToString(CultureInfo.InvariantCulture)), Convert.ToInt64(entry.Content), entry.Content);
                }

                indexWriter.Commit();
            }
        }

        Span<byte> PrepareString(string value)
        {
            if (value == null)
                return Span<byte>.Empty;
            return Encoding.UTF8.GetBytes(value);
        }

        
        private class IndexSingleEntryDouble
        {
            public string Id;
            public double Content;
        }
    }
}
