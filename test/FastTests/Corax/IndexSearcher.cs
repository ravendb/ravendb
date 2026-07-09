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
using Corax.Querying.Planning;
using Corax.Querying.Matches.SortingMatches;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;
using Raven.Server.Documents.Queries;
using Sparrow;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
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
        [RavenFact(RavenTestCategory.Corax)]
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
                    builder.EndWriting();
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
                var query = indexSearcher.BetweenQuery(binding.Metadata, double.MinValue, double.MaxValue, ComparisonOperator.GreaterThanOrEqual,
                    ComparisonOperator.LessThanOrEqual);
                Span<long> ids = stackalloc long[64];

                Assert.Equal(0, query.Fill(ids));
            }            
        }
        
        [RavenFact(RavenTestCategory.Corax)]
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
        
        [RavenFact(RavenTestCategory.Corax)]
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

                var nullResults = -1;
                var cmp = CompactKeyComparer.Compare(reader.GetTerm(ids[0]), reader.GetTerm(ids[1]), nullResults);
                Assert.Equal(string.Compare(term0, term1, StringComparison.Ordinal),Math.Sign(cmp));
                cmp = CompactKeyComparer.Compare(reader.GetTerm(ids[1]), reader.GetTerm(ids[0]), nullResults);
                Assert.Equal(string.Compare(term1, term0, StringComparison.Ordinal), Math.Sign(cmp));
                cmp = CompactKeyComparer.Compare(reader.GetTerm(ids[0]), reader.GetTerm(ids[0]), nullResults);
                Assert.Equal(string.Compare(term0, term0, StringComparison.Ordinal), Math.Sign(cmp));
                cmp = CompactKeyComparer.Compare(reader.GetTerm(ids[1]), reader.GetTerm(ids[1]), nullResults);
                Assert.Equal(string.Compare(term1, term1, StringComparison.Ordinal), Math.Sign(cmp));
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
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

        [RavenFact(RavenTestCategory.Corax)]
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

        [RavenFact(RavenTestCategory.Corax)]
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

        [RavenFact(RavenTestCategory.Corax)]
        public void EmptyAnd()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/1' AND Content = 'mountain'");
                Assert.Equal(0, results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SingleAndNoDuplication()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};


            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content IN ('road', 'lake')");
                Assert.Equal(2, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/1", "entry/2");
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SingleAnd()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/1' AND Content = 'mountain'");

                Assert.Equal(1, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/1");
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void AllAnd()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake", "mountain"},};
            var entry2 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/1' AND Content = 'mountain'");

                Assert.Equal(2, results.Count);
                Assert.NotEqual(results[0], results[1]); // two distinct index entries, not the same entry twice
                AssertIds(ResolveDocumentIds(results), "entry/1", "entry/1");
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void AllAndWithEmpty()
        {
            var entries = Enumerable.Range(1, 10_000).Select(i => new IndexEntry {Id = $"entry/{i}", Content = new string[] {"road", "lake", "mountain"}}).ToArray();


            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entries, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'Maciej'");
                Assert.Equal(0, results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void AllAndMemoized()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake", "mountain"},};
            var entry2 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/1' AND Content = 'mountain'");
                Assert.Equal(2, results.Count);
                Assert.NotEqual(results[0], results[1]); // two distinct index entries, not the same entry twice
                AssertIds(ResolveDocumentIds(results), "entry/1", "entry/1");
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void EmptyOr()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/3' OR Content = 'highway'");
                Assert.Equal(0, results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SingleOr()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                var results1 = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/1' OR Content = 'highway'");
                Assert.Equal(1, results1.Count);
                AssertIds(ResolveDocumentIds(results1), "entry/1");

                var results2 = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/3' OR Content = 'mountain'");
                Assert.Equal(1, results2.Count);
                AssertIds(ResolveDocumentIds(results2), "entry/2");
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void AllOr()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/1' OR Content = 'mountain'");
                Assert.Equal(2, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/1", "entry/2");
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void AllOrInBatches()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};
            var entry3 = new IndexEntry {Id = "entry/3", Content = new string[] {"trail", "mountain"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/1' OR Content = 'mountain'");
                Assert.Equal(3, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/1", "entry/2", "entry/3");
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleAndOr()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake", "mountain"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};
            var entry3 = new IndexEntry {Id = "entry/3", Content = new string[] {"sky", "space"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE (Id = 'entry/1' AND Content = 'mountain') OR Id = 'entry/3'");
                Assert.Equal(2, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/1", "entry/3");
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Id = 'entry/3' OR (Id = 'entry/1' AND Content = 'mountain')");
                Assert.Equal(2, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/1", "entry/3");
            }
        }


        [RavenTheory(RavenTestCategory.Corax)]
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
                var results = ExecuteRQLQuery("FROM TestIndex WHERE (Content = 'lake' AND Content = 'mountain') OR Content = 'space'");
                var sortedResults = results.ToArray();
                Array.Sort(sortedResults);

                Assert.Equal(matchesId.Count, results.Count);

                for (int i = 0; i < results.Count; i++)
                {
                    Assert.Equal(matchesId[i], sortedResults[i]);
                }
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleInStatement()
        {
            var entry1 = new IndexEntry {Id = "entry/1", Content = new string[] {"road", "lake", "mountain"},};
            var entry2 = new IndexEntry {Id = "entry/2", Content = new string[] {"road", "mountain"},};
            var entry3 = new IndexEntry {Id = "entry/3", Content = new string[] {"sky", "space"},};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content IN ('road', 'space')");
                Assert.Equal(3, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/1", "entry/2", "entry/3");
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content IN ('sky', 'space')");
                Assert.Equal(1, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/3");
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content IN ('road', 'mountain', 'space')");
                Assert.Equal(3, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/1", "entry/2", "entry/3");
            }
        }

        [RavenTheory(RavenTestCategory.Corax)]
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
            Array.Sort(matchIds);

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE (Content IN ('lake', 'mountain')) AND Content = 'sky'");
                var resultsSorted = results.ToArray();
                Array.Sort(resultsSorted);

                Assert.Equal((setSize / 3), results.Count);

                for (int i = 0; i < results.Count; i++)
                {
                    Assert.Equal(matchIds[i], resultsSorted[i]);
                }
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content = 'sky' AND (Content IN ('lake', 'mountain'))");
                var resultsSorted = results.ToArray();
                Array.Sort(resultsSorted);

                Assert.Equal((setSize / 3), results.Count);

                for (int i = 0; i < results.Count; i++)
                {
                    Assert.Equal(matchIds[i], resultsSorted[i]);
                }
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
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

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content = 'quo' AND Content = 'in'");
                Assert.Equal(2, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/0", "entry/6");
            }

            {
                // ALL IN requires every listed term to be present — only entry9 has all 27.
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content ALL IN ('dolorem', 'ipsa', 'in', 'omnis', 'ullamco', 'ab', 'esse', 'aut', 'rem', 'eu', 'iure', 'ad', 'consequuntur', 'est', 'adipisci', 'velit', 'inventore', 'nesciunt.', 'vitae', 'laborum.', 'voluptate', 'et', 'fugiat', 'voluptas', 'quae', 'dolor', 'qui')");
                Assert.Equal(1, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/9");
            }

            {
                // One term replaced with a unique value no entry has → 0 results.
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content ALL IN ('dolorem', 'ipsa', 'in', 'omnis', 'ullamco', 'ab', 'esse', 'aut', 'rem', 'eu', 'iure', 'ad', 'consequuntur', 'est', 'adipisci', 'velit', 'inventore', 'nesciunt.', 'vitae', 'laborum.', 'voluptate', 'et', 'fugiat', 'voluptas', 'quae', 'dolor', 'THIS_IS_SUPER_UNIQUE_VALUE')");
                Assert.Equal(0, results.Count);
            }
        }


        // RavenDB-25281: a backward StartsWith whose prefix has no finite successor (here {0xFF}) must seek to the
        // END of the tree and walk down, since the prefix block is the tail. Reset() positions the backward
        // iterator at the tree end. Terms are raw bytes (no analyzer) to build an all-0xFF prefix UTF-8 cannot produce.
        [RavenFact(RavenTestCategory.Corax)]
        public void BackwardStartsWith_PrefixWithNoFiniteSuccessor_WalksFromTreeEnd()
        {
            // Tree (ascending byte order): {0x10}, {0x20}, {0xFF,0x01}, {0xFF,0x02}, {0xFF,0xFF}.
            // The {0xFF} prefix matches the last three; the first two must be excluded.
            var entries = new (string Id, byte[] Term)[]
            {
                ("e/low1",  new byte[] { 0x10 }),
                ("e/low2",  new byte[] { 0x20 }),
                ("e/ff1",   new byte[] { 0xFF, 0x01 }),
                ("e/ff2",   new byte[] { 0xFF, 0x02 }),
                ("e/ffmax", new byte[] { 0xFF, 0xFF }),
            };

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            var idToTerm = new Dictionary<long, byte[]>();
            using (var indexWriter = new IndexWriter(Env, CreateKnownFields(bsc), SupportedFeatures.All))
            {
                foreach (var (id, term) in entries)
                {
                    using var builder = indexWriter.Index(id);
                    builder.Write(IdIndex, PrepareString(id));
                    builder.Write(ContentIndex, term);
                    idToTerm[(long)builder.EntryId] = term;
                    builder.EndWriting();
                }

                indexWriter.Commit();
            }

            static int Compare(byte[] a, byte[] b) => a.AsSpan().SequenceCompareTo(b);

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var contentMeta = searcher.FieldMetadataBuilder("Content", ContentIndex);
            Slice.From(Allocator, new byte[] { 0xFF }, out var prefix);

            // Backward provider, wrapped in SortedDrivingMatch (as the planner does) so it yields in term order:
            // exactly the three 0xFF-prefixed entries, in DESCENDING order, having walked from the tree end.
            {
                var match = searcher.StartWithQuery(contentMeta, prefix, forward: false);
                var tpm = Assert.IsType<TermsProviderMatch>(match);
                using var sorted = new SortedDrivingMatch(tpm.Provider, tpm.Llt, Allocator);

                Span<long> ids = stackalloc long[16];
                int n = sorted.Fill(ids);
                Assert.Equal(3, n);
                var got = ids[..n].ToArray().Select(id => idToTerm[id]).ToList();
                Assert.All(got, t => Assert.Equal(0xFF, t[0])); // the {0x10}/{0x20} entries must not leak in
                for (int i = 1; i < got.Count; i++)
                    Assert.True(Compare(got[i - 1], got[i]) > 0, "Backward StartsWith must yield terms in descending order");
                Assert.Equal(0, sorted.Fill(ids));
            }

            // Forward provider over the same prefix: the seek-limit change must leave the forward path intact —
            // same three entries, ASCENDING.
            {
                var match = searcher.StartWithQuery(contentMeta, prefix, forward: true);
                var tpm = Assert.IsType<TermsProviderMatch>(match);
                using var sorted = new SortedDrivingMatch(tpm.Provider, tpm.Llt, Allocator);

                Span<long> ids = stackalloc long[16];
                int n = sorted.Fill(ids);
                Assert.Equal(3, n);
                var got = ids[..n].ToArray().Select(id => idToTerm[id]).ToList();
                Assert.All(got, t => Assert.Equal(0xFF, t[0]));
                for (int i = 1; i < got.Count; i++)
                    Assert.True(Compare(got[i - 1], got[i]) < 0, "Forward StartsWith must yield terms in ascending order");
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
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

        [RavenFact(RavenTestCategory.Corax)]
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
                var match = searcher.OrderBy(match1, orderMetadata, take: 16, defaultNullsSortMode: NullsSortMode.NullsSmallest);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }
        }


        [RavenFact(RavenTestCategory.Corax)]
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
                var match = searcher.OrderBy(match1, orderMetadata, defaultNullsSortMode: NullsSortMode.NullsSmallest);

                Span<long> ids = stackalloc long[2];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));

                Assert.Equal(3, match.TotalResults);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
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

        [RavenFact(RavenTestCategory.Corax)]
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
                var match = searcher.OrderBy(match1, orderMetadata, defaultNullsSortMode: NullsSortMode.NullsSmallest);

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
                var match = searcher.OrderBy(match1, orderMetadata, take: 16, defaultNullsSortMode: NullsSortMode.NullsSmallest);

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

        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleOrdinalCompareStatementWithLongValue()
        {
            var list = new List<IndexSingleEntryDouble>();
            for (int i = 1; i < 1001; ++i)
                list.Add(new IndexSingleEntryDouble() {Id = $"entry/{i}", Content = (double)i});
            List<string> qids = new();
            IndexEntriesDouble(list);
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content < 25");
                qids = results.Select(id => searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id)).ToList();

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
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content >= 100 AND Content <= 200");
                qids = results.Select(id => searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id)).ToList();

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


        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleOrdinalCompareStatement()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "3"};
            var entry2 = new IndexSingleEntry {Id = "entry/2", Content = "2"};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "1"};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content > '1'");
                Assert.Equal(2, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/1", "entry/2");
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content >= '1'");
                Assert.Equal(3, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/1", "entry/2", "entry/3");
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content < '1'");
                Assert.Equal(0, results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content <= '1'");
                Assert.Equal(1, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/3");
            }
        }


        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleEqualityCompareStatement()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "1"};
            var entry2 = new IndexSingleEntry {Id = "entry/2", Content = "2"};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "1"};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content = '1'");
                Assert.Equal(2, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/1", "entry/3");
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content != '1'");
                Assert.Equal(1, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/2");
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content = '4'");
                Assert.Equal(0, results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content != '4'");
                Assert.Equal(3, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/1", "entry/2", "entry/3");
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
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

        [RavenFact(RavenTestCategory.Corax)]
        public void SimpleBetweenCompareStatement()
        {
            var entry1 = new IndexSingleEntry {Id = "entry/1", Content = "3"};
            var entry2 = new IndexSingleEntry {Id = "entry/2", Content = "2"};
            var entry3 = new IndexSingleEntry {Id = "entry/3", Content = "1"};
            var entry4 = new IndexSingleEntry {Id = "entry/4", Content = "4"};

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] {entry1, entry2, entry3, entry4}, CreateKnownFields(bsc));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content >= '1' AND Content <= '2'");
                Assert.Equal(2, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/2", "entry/3");
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content >= '0' AND Content <= '3'");
                Assert.Equal(3, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/1", "entry/2", "entry/3");
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content >= '0' AND Content <= '0'");
                Assert.Equal(0, results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content >= '1' AND Content <= '1'");
                Assert.Equal(1, results.Count);
                AssertIds(ResolveDocumentIds(results), "entry/3");
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void BetweenWithCustomComparers()
        {
            var entries = Enumerable.Range(0, 100).Select(i => new IndexSingleEntryDouble() {Id = $"entry{i}", Content = Convert.ToDouble(i)}).ToList();
            IndexEntriesDouble(entries);
            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content >= 20 AND Content <= 30");
                var expected = entries.Where(i => i.Content is >= 20 and <= 30).Select(e => e.Id).ToArray();
                Assert.Equal(expected.Length, results.Count);
                AssertIds(ResolveDocumentIds(results), expected);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content > 20 AND Content <= 30");
                var expected = entries.Where(i => i.Content is > 20 and <= 30).Select(e => e.Id).ToArray();
                Assert.Equal(expected.Length, results.Count);
                AssertIds(ResolveDocumentIds(results), expected);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content >= 20 AND Content < 30");
                var expected = entries.Where(i => i.Content is >= 20 and < 30).Select(e => e.Id).ToArray();
                Assert.Equal(expected.Length, results.Count);
                AssertIds(ResolveDocumentIds(results), expected);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content > 20 AND Content < 30");
                var expected = entries.Where(i => i.Content is > 20 and < 30).Select(e => e.Id).ToArray();
                Assert.Equal(expected.Length, results.Count);
                AssertIds(ResolveDocumentIds(results), expected);
            }
        }
        
        [RavenTheory(RavenTestCategory.Corax)]
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

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content IN ('lake', 'mountain') AND Content = 'sky'");
                Assert.Equal((setSize / 3), results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content = 'sky' AND Content IN ('lake', 'mountain')");
                Assert.Equal((setSize / 3), results.Count);
            }
        }

        [RavenTheory(RavenTestCategory.Corax)]
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

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content IN ('lake', 'mountain') AND Content = 'sky'");
                Assert.Equal((setSize / 3), results.Count);
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content = 'sky' AND Content IN ('lake', 'mountain')");
                Assert.Equal((setSize / 3), results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
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

            {
                var inList = string.Join("', '", listForNotIn.Select(l => l.Content));
                // RQL requires "true AND NOT" to express negation at the top level.
                // "true AND NOT Content IN (...)" is the valid form for NOT IN.
                var results = ExecuteRQLQuery($"FROM TestIndex WHERE true AND NOT Content IN ('{inList}')");
                Assert.Equal(1000 - listForNotIn.Count(), results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void EmptyInDoesNotPoisonCacheForNonEmptyExecution()
        {
            // Regression test: an empty IN parameter ($p=[]) in an AND chain must not
            // cache a plan that subsequent non-empty executions ($p=['x']) would reuse,
            // producing zero results instead of the correct matches.
            var list = new[]
            {
                new IndexSingleEntry { Id = "entry/1", Content = "Alpha" },
                new IndexSingleEntry { Id = "entry/2", Content = "Beta" },
                new IndexSingleEntry { Id = "entry/3", Content = "Gamma" },
            };
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, list, CreateKnownFields(bsc));

            const string rql = "FROM TestIndex WHERE Content IN ($p0)";

            using var fields = CreateKnownFields(Allocator);
            using var searcher = new IndexSearcher(Env, fields);
            using var ctx = global::Sparrow.Json.JsonOperationContext.ShortTermSingleUse();

            // Execution 1: $p0 = [] (empty array) → should return 0 results
            {
                var emptyParams = ctx.ReadObject(new global::Sparrow.Json.Parsing.DynamicJsonValue { ["p0"] = new global::Sparrow.Json.Parsing.DynamicJsonArray() }, "params");
                var queryMetadata = new QueryMetadata(rql, emptyParams, 0);
                var planParams = new PlanParameters
                {
                    IndexSearcher = searcher, Metadata = queryMetadata,
                    QueryParameters = emptyParams, Allocator = Allocator
                };
                var match = QueryPlanBuilder.BuildFilterMatch(planParams, new QueryBuilderParameters(searcher, Allocator, queryMetadata, emptyParams, fields), null, false, default);
                Span<long> buf = stackalloc long[64];
                Assert.Equal(0, match.Fill(buf));
            }

            // Execution 2: $p0 = ['Alpha'] → should return 1 result (not 0)
            {
                var realParams = ctx.ReadObject(new global::Sparrow.Json.Parsing.DynamicJsonValue { ["p0"] = new global::Sparrow.Json.Parsing.DynamicJsonArray { "Alpha" } }, "params");
                var queryMetadata = new QueryMetadata(rql, realParams, 0);
                var planParams = new PlanParameters
                {
                    IndexSearcher = searcher, Metadata = queryMetadata,
                    QueryParameters = realParams, Allocator = Allocator
                };
                var match = QueryPlanBuilder.BuildFilterMatch(planParams, new QueryBuilderParameters(searcher, Allocator, queryMetadata, realParams, fields), null, false, default);
                Span<long> buf = stackalloc long[64];
                Assert.Equal(1, match.Fill(buf));
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void EmptyInDoesNotPoisonCacheInOrChain()
        {
            // Regression: an empty IN parameter ($p=[]) in an OR chain must not cache a plan that subsequent
            // non-empty executions ($p=['x']) would reuse with the IN clause compacted out. The cache key does
            // not encode IN-array size, so the runtime must handle empty-IN via InRangeCounts[i]=0.
            var list = new[]
            {
                new IndexSingleEntry { Id = "entry/1", Content = "Alpha" },
                new IndexSingleEntry { Id = "entry/2", Content = "Beta" },
                new IndexSingleEntry { Id = "entry/3", Content = "Gamma" },
            };
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, list, CreateKnownFields(bsc));

            const string rql = "FROM TestIndex WHERE Content IN ($p0) OR Id = 'entry/1'";

            using var fields = CreateKnownFields(Allocator);
            using var searcher = new IndexSearcher(Env, fields);
            using var ctx = global::Sparrow.Json.JsonOperationContext.ShortTermSingleUse();

            // Execution 1: $p0 = [] (empty array) → IN side contributes nothing, OR with Id='entry/1' → 1 result
            {
                var emptyParams = ctx.ReadObject(new global::Sparrow.Json.Parsing.DynamicJsonValue { ["p0"] = new global::Sparrow.Json.Parsing.DynamicJsonArray() }, "params");
                var queryMetadata = new QueryMetadata(rql, emptyParams, 0);
                var planParams = new PlanParameters
                {
                    IndexSearcher = searcher, Metadata = queryMetadata,
                    QueryParameters = emptyParams, Allocator = Allocator
                };
                var match = QueryPlanBuilder.BuildFilterMatch(planParams, new QueryBuilderParameters(searcher, Allocator, queryMetadata, emptyParams, fields), null, false, default);
                Span<long> buf = stackalloc long[64];
                Assert.Equal(1, match.Fill(buf));
            }

            // Execution 2: $p0 = ['Beta'] → IN matches entry/2, OR Id='entry/1' → 2 results.
            // Would return 1 if the cached plan from execution 1 dropped the IN clause.
            {
                var realParams = ctx.ReadObject(new global::Sparrow.Json.Parsing.DynamicJsonValue { ["p0"] = new global::Sparrow.Json.Parsing.DynamicJsonArray { "Beta" } }, "params");
                var queryMetadata = new QueryMetadata(rql, realParams, 0);
                var planParams = new PlanParameters
                {
                    IndexSearcher = searcher, Metadata = queryMetadata,
                    QueryParameters = realParams, Allocator = Allocator
                };
                var match = QueryPlanBuilder.BuildFilterMatch(planParams, new QueryBuilderParameters(searcher, Allocator, queryMetadata, realParams, fields), null, false, default);
                Span<long> buf = stackalloc long[64];
                Assert.Equal(2, match.Fill(buf));
            }
        }

        // Compile (and cache) a plan for an RQL filter against the given searcher. Returns the QueryMetadata used,
        // so a caller can reuse the same instance across builds to exercise the QueryMetadata plan memo.
        private QueryMetadata BuildPlanForCacheTest(IndexSearcher searcher, IndexFieldsMapping fields, JsonOperationContext ctx,
            string rql, global::Sparrow.Json.Parsing.DynamicJsonValue paramsJson, QueryMetadata metadata = null)
        {
            var queryParams = ctx.ReadObject(paramsJson, "params");
            metadata ??= new QueryMetadata(rql, queryParams, 0);
            var planParams = new PlanParameters
            {
                IndexSearcher = searcher, Metadata = metadata,
                QueryParameters = queryParams, Allocator = Allocator
            };
            var match = QueryPlanBuilder.BuildFilterMatch(planParams,
                new QueryBuilderParameters(searcher, Allocator, metadata, queryParams, fields), null, false, default);
            Span<long> buf = stackalloc long[64];
            match.Fill(buf); // drive execution; result count is irrelevant to plan-cache identity
            return metadata;
        }

        private static IndexSingleEntry[] PlanCacheSeed() =>
        [
            new IndexSingleEntry { Id = "entry/1", Content = "Alpha" },
            new IndexSingleEntry { Id = "entry/2", Content = "Beta" },
        ];

        // Plan-cache bucketing: the per-query bucket is keyed by a structural key (SHA256 of the query text),
        // not a string. Two executions of the SAME query text (values are never part of the key) resolve to ONE
        // bucket holding ONE compiled plan variant.
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void PlanCache_SameQueryText_SharesSingleBucketAndPlan()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, PlanCacheSeed(), CreateKnownFields(bsc));

            using var fields = CreateKnownFields(Allocator);
            using var searcher = new IndexSearcher(Env, fields);
            using var ctx = JsonOperationContext.ShortTermSingleUse();

            const string rql = "FROM TestIndex WHERE Content = $p0";

            // Fresh QueryMetadata each time so the memo doesn't short-circuit — this exercises the GetBucket path.
            BuildPlanForCacheTest(searcher, fields, ctx, rql, new() { ["p0"] = "Alpha" });
            BuildPlanForCacheTest(searcher, fields, ctx, rql, new() { ["p0"] = "Beta" });

            var snapshot = searcher.PlanCache.Snapshot();
            Assert.Single(snapshot);
            Assert.Equal(rql, snapshot[0].QueryText);
            Assert.Single(snapshot[0].Plans); // same param type → one inner variant
        }

        // Same query text, but the bound parameter's RUNTIME TYPE differs (string vs long). The structural key is
        // identical (one bucket) while the inner 256-bit key encodes the per-parameter type, so the two type
        // variants live as TWO compiled plans inside the SAME bucket.
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void PlanCache_SameText_DifferentParamType_SplitsWithinOneBucket()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, PlanCacheSeed(), CreateKnownFields(bsc));

            using var fields = CreateKnownFields(Allocator);
            using var searcher = new IndexSearcher(Env, fields);
            using var ctx = JsonOperationContext.ShortTermSingleUse();

            const string rql = "FROM TestIndex WHERE Content = $p0";

            BuildPlanForCacheTest(searcher, fields, ctx, rql, new() { ["p0"] = "Alpha" }); // string
            BuildPlanForCacheTest(searcher, fields, ctx, rql, new() { ["p0"] = 5L });      // long

            var snapshot = searcher.PlanCache.Snapshot();
            Assert.Single(snapshot); // one bucket (one structural key)
            Assert.Equal(2, snapshot[0].Plans.Length); // two inner type variants
        }

        // Different query texts hash to different structural keys → distinct buckets.
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void PlanCache_DifferentQueryText_DistinctBuckets()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, PlanCacheSeed(), CreateKnownFields(bsc));

            using var fields = CreateKnownFields(Allocator);
            using var searcher = new IndexSearcher(Env, fields);
            using var ctx = JsonOperationContext.ShortTermSingleUse();

            BuildPlanForCacheTest(searcher, fields, ctx, "FROM TestIndex WHERE Content = $p0", new() { ["p0"] = "Alpha" });
            BuildPlanForCacheTest(searcher, fields, ctx, "FROM TestIndex WHERE Id = $p0", new() { ["p0"] = "entry/1" });

            var snapshot = searcher.PlanCache.Snapshot();
            Assert.Equal(2, snapshot.Count);
        }

        // The QueryMetadata plan memo lets the hot path skip the dictionary entirely. After the first build it is
        // stamped with the live cache's Generation and weakly holds the resolved bucket; a second build that reuses
        // the SAME QueryMetadata takes the fast path and never reallocates the memo (object identity preserved).
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void PlanCache_Memo_ReusedAcrossBuilds_AndStampedWithCacheId()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, PlanCacheSeed(), CreateKnownFields(bsc));

            using var fields = CreateKnownFields(Allocator);
            using var searcher = new IndexSearcher(Env, fields);
            using var ctx = JsonOperationContext.ShortTermSingleUse();

            const string rql = "FROM TestIndex WHERE Content = $p0";

            var metadata = BuildPlanForCacheTest(searcher, fields, ctx, rql, new() { ["p0"] = "Alpha" });
            var memo = metadata.CachedPlanMemo;
            Assert.NotNull(memo);
            Assert.Equal(searcher.PlanCache.GenerationIdx, memo.PlanCacheGeneration);
            Assert.True(memo.Bucket.TryGetTarget(out _));

            // Reuse the same QueryMetadata: the memo fast path is taken, so the memo object is not replaced.
            BuildPlanForCacheTest(searcher, fields, ctx, rql, new() { ["p0"] = "Beta" }, metadata);
            Assert.Same(memo, metadata.CachedPlanMemo);
        }

        // An index swap replaces the IndexSearcher (and its PlanCache, which carries a fresh Generation). A
        // QueryMetadata memo stamped against the OLD cache must be rejected by the generation compare and re-resolved
        // against the new cache, re-stamping the memo with the new generation — otherwise a stale bucket from a
        // replaced index would be used.
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
        public void PlanCache_IndexSwap_InvalidatesMemo_AndRebuilds()
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, PlanCacheSeed(), CreateKnownFields(bsc));

            using var fields = CreateKnownFields(Allocator);
            using var ctx = JsonOperationContext.ShortTermSingleUse();

            const string rql = "FROM TestIndex WHERE Content = $p0";
            QueryMetadata metadata;
            long oldCacheId;

            using (var searcherA = new IndexSearcher(Env, fields))
            {
                metadata = BuildPlanForCacheTest(searcherA, fields, ctx, rql, new() { ["p0"] = "Alpha" });
                oldCacheId = searcherA.PlanCache.GenerationIdx;
                Assert.Equal(oldCacheId, metadata.CachedPlanMemo.PlanCacheGeneration);
            }

            // Simulate the index swap: a new searcher with its own PlanCache (distinct Generation), reusing the metadata.
            using var searcherB = new IndexSearcher(Env, fields);
            Assert.NotEqual(oldCacheId, searcherB.PlanCache.GenerationIdx);

            BuildPlanForCacheTest(searcherB, fields, ctx, rql, new() { ["p0"] = "Beta" }, metadata);

            Assert.Equal(searcherB.PlanCache.GenerationIdx, metadata.CachedPlanMemo.PlanCacheGeneration); // re-stamped against the new cache
            Assert.Single(searcherB.PlanCache.Snapshot()); // the new cache compiled its own bucket
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

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            {
                // true AND NOT ... anchors the top-level NOT (NOT alone is parsed as a method call).
                var results = ExecuteRQLQuery("FROM TestIndex WHERE true AND NOT startsWith(Content, 'Run')");
                Assert.Equal(1, results.Count);
                var item = searcher.TermsReaderFor("Id").GetTermFor(results[0]);
                Assert.Equal("entry/1", item);
            }

            {
                // Empty result case: exclude all 3 known IDs → 0 results
                var notAllResults = ExecuteRQLQuery("FROM TestIndex WHERE Id != 'entry/1' AND Id != 'entry/2' AND Id != 'entry/3'");
                Assert.Equal(0, notAllResults.Count);
            }

            {
                // No entries start with 'J', so true AND NOT startsWith(Content, 'J') keeps all.
                var results = ExecuteRQLQuery("FROM TestIndex WHERE true AND NOT startsWith(Content, 'J')");
                Assert.Equal(3, results.Count);
                var uniqueIds = new HashSet<long>(results);
                Assert.Equal(3, uniqueIds.Count);
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

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entriesToIndex, CreateKnownFields(bsc));

            var allIds = entries.Select(e => e.Id).ToArray();

            {
                // Test: (Content != '8') OR (Content != '9') OR (Content != '10') = all entries
                // (no entry has 8, 9, or 10, so each != matches all 7)
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content != '8' OR Content != '9' OR Content != '10'");
                Assert.Equal(7, results.Count);
                AssertIds(ResolveDocumentIds(results), allIds);
            }

            {
                // Test: NOT (1 IN doc OR 2 IN doc OR ... OR 7 IN doc) means
                // NOT (1 IN doc) OR NOT (2 IN doc) ... — at least one value absent from the set.
                // No entry has ALL of {1,2,3,5,7}, so OR of NOT-in gives all 7 entries.
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content != '1' OR Content != '2' OR Content != '3' OR Content != '5' OR Content != '7'");
                Assert.Equal(entries.Count, results.Count);
                AssertIds(ResolveDocumentIds(results), allIds);
            }

            {
                // Test: All entries have an Id starting with 'entry/'
                var results = ExecuteRQLQuery("FROM TestIndex WHERE startsWith(Id, 'entry/')");
                Assert.Equal(7, results.Count);
                AssertIds(ResolveDocumentIds(results), allIds);
            }

            {
                // Test: startsWith(Id, 'entry/') AND NOT Content IN ('8', '9', '10') = all 7
                // None of the entries have content values 8, 9, or 10.
                var results = ExecuteRQLQuery("FROM TestIndex WHERE startsWith(Id, 'entry/') AND NOT Content IN ('8', '9', '10')");
                Assert.Equal(7, results.Count);
                AssertIds(ResolveDocumentIds(results), allIds);
            }
        }

        [RavenTheory(RavenTestCategory.Corax)]
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

            {
                //MultiTermMatch And TermMatch
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content IN ('maciej', 'poland') AND Content = 'this'");
                var resultByLinq = entries.Where(x => (x.Content.Contains("maciej") || x.Content.Contains("poland")) && x.Content.Contains("this")).ToList();
                AssertIds(ResolveDocumentIds(results), resultByLinq.Select(e => e.Id).ToArray());
            }

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE startsWith(Content, 'ma') OR Content = 'torun'");
                var linqResult = entries.Where(x => x.Content.Any(z => z.StartsWith("ma") || z.Contains("torun"))).ToList();
                AssertIds(ResolveDocumentIds(results), linqResult.Select(e => e.Id).ToArray());
            }

            string[] GetContent()
            {
                var amount = random.Next(0, 10);
                return Enumerable.Range(0, amount).Select(i => words[random.Next(0, words.Count())]).ToArray();
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
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

            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content != '1'");
                Assert.Equal(3, results.Count);
            }
            {
                var results = ExecuteRQLQuery("FROM TestIndex WHERE Content != '2'");
                var expected = entries.Count(x => x.Content.Contains("2") == false);
                Assert.Equal(expected, results.Count);
            }
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void NoWhereClauseIsNotAlwaysEmpty()
        {
            var entry1 = new IndexEntry { Id = "entry/1", Content = new string[] { "road", "lake" } };
            var entry2 = new IndexEntry { Id = "entry/2", Content = new string[] { "muddy", "road" } };

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] { entry1, entry2 }, CreateKnownFields(bsc));

            // A no-WHERE query returns all entries.
            var results = ExecuteRQLQuery("FROM TestIndex");
            Assert.Equal(2, results.Count);
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void WhenFalseEliminatesAllClausesReturnAllEntries()
        {
            var entry1 = new IndexEntry { Id = "entry/1", Content = new string[] { "road", "lake" } };
            var entry2 = new IndexEntry { Id = "entry/2", Content = new string[] { "muddy", "road" } };

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, new[] { entry1, entry2 }, CreateKnownFields(bsc));

            // when($p = true, Content = 'road') with $p = false → clause eliminated.
            // With all clauses removed, the query reduces to match-all (matches Lucene's
            // LuceneWhenQuery → MatchAllDocsQuery() at top level).
            using var fields = CreateKnownFields(Allocator);
            using var searcher = new IndexSearcher(Env, fields);
            var rql = "FROM TestIndex WHERE when($p = true, Content = 'road')";
            var queryMetadata = new QueryMetadata(rql, null, 0);

            using var ctx = global::Sparrow.Json.JsonOperationContext.ShortTermSingleUse();
            var paramsJson = ctx.ReadObject(new global::Sparrow.Json.Parsing.DynamicJsonValue { ["p"] = false }, "params");

            var planParams = new PlanParameters
            {
                IndexSearcher = searcher,
                Metadata = queryMetadata,
                QueryParameters = paramsJson,
                Allocator = Allocator
            };

            var match = QueryPlanBuilder.BuildFilterMatch(planParams, new QueryBuilderParameters(searcher, Allocator, queryMetadata, paramsJson, fields), null, false, default);
            Span<long> buffer = stackalloc long[256];
            int count = match.Fill(buffer);
            Assert.Equal(2, count);
        }

        /// <summary>
        /// Executes an RQL query through QueryPlanBuilder (RQL → AST → IL compilation → execution) and returns matching entry IDs.
        /// </summary>
        private List<long> ExecuteRQLQuery(string rqlQuery)
        {
            using var fields = CreateKnownFields(Allocator);
            using var searcher = new IndexSearcher(Env, fields);
            return CoraxRqlTestHelper.ExecuteRQLQuery(searcher, Allocator, fields, rqlQuery);
        }

        /// <summary>
        /// Resolves a list of Corax entry IDs to their document ID strings (preserving duplicates).
        /// </summary>
        private List<string> ResolveDocumentIds(List<long> entryIds)
        {
            // Verify the engine didn't return the same internal entry ID twice — distinct entries
            // may legitimately share the same document Id field value (e.g. "entry/1" twice), but
            // the underlying long entry IDs must be unique.
            var seen = new HashSet<long>(entryIds.Count);
            foreach (long id in entryIds)
                Assert.True(seen.Add(id), $"Engine returned the same entry ID twice: {id}");

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));
            var result = new List<string>(entryIds.Count);
            foreach (long id in entryIds)
                result.Add(searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
            result.Sort(StringComparer.Ordinal);
            return result;
        }

        /// <summary>
        /// Asserts that the resolved document IDs match the expected set exactly (order-independent).
        /// </summary>
        private static void AssertIds(List<string> actual, params string[] expected)
        {
            var sorted = (string[])expected.Clone();
            Array.Sort(sorted, StringComparer.Ordinal);
            Assert.Equal(sorted.Length, actual.Count);
            for (int i = 0; i < sorted.Length; i++)
                Assert.Equal(sorted[i], actual[i]);
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

                entry.IndexEntryId = (long)builder.EntryId;
                builder.EndWriting();
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
                builder.EndWriting();
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
                    entryWriter.EndWriting();
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

        [RavenFact(RavenTestCategory.Corax)]
        public void RandomOrderOnBitmapMatchProducesActualRandomOrder()
        {
            // Regression: the bitmap path for ORDER BY random() sorted by term name (SortInMemory<EntryComparerByTerm>)
            // instead of shuffling via SampleRandomOrder. Uses BitmapMatch directly to stay independent of QueryILEmitter.

            const int N = 32;
            var entries = Enumerable.Range(1, N)
                .Select(i => new IndexSingleEntry { Id = $"entry/{i:D3}", Content = i.ToString() })
                .ToList();

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entries, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            // Collect all entry IDs from a plain TermQuery that matches all docs.
            // We use ExistsQuery to get all entry IDs, then build a BitmapMatch from them.
            var allEntryIds = new List<long>();
            {
                Span<long> buf = stackalloc long[256];
                var exists = searcher.ExistsQuery(searcher.FieldMetadataBuilder("Content", ContentIndex));
                int r;
                while ((r = exists.Fill(buf)) > 0)
                    for (int i = 0; i < r; i++)
                        allEntryIds.Add(buf[i]);
            }

            Assert.Equal(N, allEntryIds.Count);
            var allEntryIdsSorted = allEntryIds.OrderBy(x => x).ToList();

            static List<long> RunOrder(IndexSearcher searcher, List<long> allEntryIds, ByteStringContext allocator, int seed)
            {
                // Build a BitmapMatch from the known entry IDs — this implements IBitmapQueryMatch,
                // which triggers the bitmap-specific code path in SortingMatch.
                var bitmapMatch = new BitmapMatch(allocator);
                foreach (long id in allEntryIds)
                    bitmapMatch.BitmapState.Add(id);

                var orderMeta = new OrderMetadata(seed);
                var sortMatch = searcher.OrderBy(bitmapMatch, orderMeta, defaultNullsSortMode: NullsSortMode.NullsLargest);
                var results = new List<long>();
                Span<long> buf = stackalloc long[256];
                int r;
                while ((r = sortMatch.Fill(buf)) > 0)
                    for (int i = 0; i < r; i++)
                        results.Add(buf[i]);
                return results;
            }

            // Same seed → identical order both times.
            var run1 = RunOrder(searcher, allEntryIds, Allocator, seed: 42);
            var run2 = RunOrder(searcher, allEntryIds, Allocator, seed: 42);
            Assert.Equal(run1, run2);

            // Different seed → different order (with 32 entries this is virtually certain).
            var run3 = RunOrder(searcher, allEntryIds, Allocator, seed: 99);
            Assert.NotEqual(run1, run3);

            // Must be a permutation — nothing lost, nothing duplicated.
            Assert.Equal(allEntryIdsSorted, run1.OrderBy(x => x).ToList());

            // Must NOT be in ascending entry-ID order — that was the bug (term sort behaviour).
            Assert.NotEqual(allEntryIdsSorted, run1);
        }

        [RavenFact(RavenTestCategory.Corax)]
        public void RandomOrderOnBitmapMatchWithTakeSelectsCorrectSubset()
        {
            // Verify LIMIT is respected: reservoir sampling must return exactly _take
            // distinct entries, all from the original set.
            const int N = 32;
            const int Take = 7;

            var entries = Enumerable.Range(1, N)
                .Select(i => new IndexSingleEntry { Id = $"entry/{i:D3}", Content = i.ToString() })
                .ToList();

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            IndexEntries(bsc, entries, CreateKnownFields(bsc));

            using var searcher = new IndexSearcher(Env, CreateKnownFields(Allocator));

            var allEntryIds = new HashSet<long>();
            {
                Span<long> buf = stackalloc long[256];
                var exists = searcher.ExistsQuery(searcher.FieldMetadataBuilder("Content", ContentIndex));
                int r;
                while ((r = exists.Fill(buf)) > 0)
                    for (int i = 0; i < r; i++)
                        allEntryIds.Add(buf[i]);
            }

            var bitmapMatch = new BitmapMatch(Allocator);
            foreach (long id in allEntryIds)
                bitmapMatch.BitmapState.Add(id);

            var orderMeta = new OrderMetadata(7);
            var sortMatch = searcher.OrderBy(bitmapMatch, orderMeta, defaultNullsSortMode: NullsSortMode.NullsLargest, take: Take);
            var results = new List<long>();
            Span<long> buf2 = stackalloc long[256];
            int read;
            while ((read = sortMatch.Fill(buf2)) > 0)
                for (int i = 0; i < read; i++)
                    results.Add(buf2[i]);

            Assert.Equal(Take, results.Count);
            Assert.All(results, id => Assert.Contains(id, allEntryIds));
            Assert.Equal(Take, results.Distinct().Count());
        }
    }
}
