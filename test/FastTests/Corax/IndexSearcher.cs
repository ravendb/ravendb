using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using Corax.Pipeline;
using FastTests.Voron;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax
{
    public class IndexSearcherTest : StorageTest
    {
        private class IndexEntry
        {
            public string Id;
            public string[] Content;
        }

        private class IndexSingleEntry
        {
            public string Id;
            public string Content;
        }

        private readonly struct StringArrayIterator : IReadOnlySpanEnumerator
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

            public ReadOnlySpan<byte> this[int i] => Encoding.UTF8.GetBytes(_values[i]);
        }

        private static Span<byte> CreateIndexEntry(ref IndexEntryWriter entryWriter, IndexEntry value)
        {
            Span<byte> PrepareString(string value)
            {
                if (value == null)
                    return Span<byte>.Empty;
                return Encoding.UTF8.GetBytes(value);
            }

            entryWriter.Write(IdIndex, PrepareString(value.Id));
            entryWriter.Write(ContentIndex, new StringArrayIterator(value.Content));

            entryWriter.Finish(out var output);
            return output;
        }

        public const int IdIndex = 0,
            ContentIndex = 1;

        private static Dictionary<Slice, int> CreateKnownFields(ByteStringContext ctx)
        {
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            return new Dictionary<Slice, int> { [idSlice] = IdIndex, [contentSlice] = ContentIndex, };
        }


        public IndexSearcherTest(ITestOutputHelper output) : base(output)
        {
        }


        private void IndexEntries(IEnumerable<IndexEntry> list, Analyzer analyzer = null)
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            Dictionary<Slice, int> knownFields = CreateKnownFields(bsc);

            const int bufferSize = 4096;
            using var _ = bsc.Allocate(bufferSize, out ByteString buffer);

            {
                using var indexWriter = new IndexWriter(Env, analyzer);
                foreach (var entry in list)
                {
                    var entryWriter = new IndexEntryWriter(buffer.ToSpan(), knownFields);
                    var data = CreateIndexEntry(ref entryWriter, entry);
                    indexWriter.Index(entry.Id, data, knownFields);
                }

                indexWriter.Commit();
            }
        }

        [Fact]
        public void EmptyTerm()
        {
            var entry = new IndexEntry { Id = "entry/1", Content = new string[] { "road", "lake" }, };
            IndexEntries(new[] { entry });

            {
                Span<long> ids = stackalloc long[16];

                using var searcher = new IndexSearcher(Env);
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
            var entry = new IndexEntry { Id = "entry/1", Content = new string[] { "road", "lake" }, };
            IndexEntries(new[] { entry });

            {
                Span<long> ids = stackalloc long[16];

                using var searcher = new IndexSearcher(Env);
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
                entries[i] = new IndexEntry { Id = $"entry/{i}", Content = new string[] { "road" }, };
            }

            IndexEntries(entries);

            {
                Span<long> ids = stackalloc long[12];
                ids.Fill(-1);

                using var searcher = new IndexSearcher(Env);
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
        public void SetTerm()
        {
            var entries = new IndexEntry[100000];
            var content = new string[] { "road" };

            for (int i = 0; i < entries.Length; i++)
            {
                entries[i] = new IndexEntry { Id = $"entry/{i}", Content = content, };
            }

            IndexEntries(entries);

            {
                using var searcher = new IndexSearcher(Env);
                var match = searcher.TermQuery("Content", "road");

                Assert.Equal(100000, match.Count);

                Span<long> ids = stackalloc long[1000];
                ids.Fill(-1);

                int read;
                int total = 0;
                do
                {
                    read = match.Fill(ids);
                    if (read != 0)
                    {
                        Assert.Equal(1000, read);
                        Assert.False(ids.Contains(-1));
                        Assert.True(total <= match.Count);

                        ids.Fill(-1);
                    }

                    total += read;
                } while (read != 0);

                Assert.Equal(match.Count, total);
                Assert.Equal(0, match.Fill(ids));
            }
        }


        [Fact]
        public void EmptyAnd()
        {
            var entry1 = new IndexEntry { Id = "entry/1", Content = new string[] { "road", "lake" }, };
            var entry2 = new IndexEntry { Id = "entry/2", Content = new string[] { "road", "mountain" }, };

            IndexEntries(new[] { entry1, entry2 });

            {
                using var searcher = new IndexSearcher(Env);
                var match1 = searcher.TermQuery("Id", "entry/1");
                var match2 = searcher.TermQuery("Content", "mountain");
                var andMatch = searcher.And(in match1, in match2);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(0, andMatch.Fill(ids));
            }
        }

        [Fact]
        public void SingleAnd()
        {
            var entry1 = new IndexEntry { Id = "entry/1", Content = new string[] { "road", "lake" }, };
            var entry2 = new IndexEntry { Id = "entry/1", Content = new string[] { "road", "mountain" }, };

            IndexEntries(new[] { entry1, entry2 });

            {
                using var searcher = new IndexSearcher(Env);
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
            var entry1 = new IndexEntry { Id = "entry/1", Content = new string[] { "road", "lake", "mountain" }, };
            var entry2 = new IndexEntry { Id = "entry/1", Content = new string[] { "road", "mountain" }, };

            IndexEntries(new[] { entry1, entry2 });

            {
                using var searcher = new IndexSearcher(Env);
                var match1 = searcher.TermQuery("Id", "entry/1");
                var match2 = searcher.TermQuery("Content", "mountain");
                var andMatch = searcher.And(in match1, in match2);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, andMatch.Fill(ids));
                Assert.Equal(0, andMatch.Fill(ids));
            }
        }

        [Fact]
        public void EmptyOr()
        {
            var entry1 = new IndexEntry { Id = "entry/1", Content = new string[] { "road", "lake" }, };
            var entry2 = new IndexEntry { Id = "entry/2", Content = new string[] { "road", "mountain" }, };

            IndexEntries(new[] { entry1, entry2 });

            {
                using var searcher = new IndexSearcher(Env);
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
            var entry1 = new IndexEntry { Id = "entry/1", Content = new string[] { "road", "lake" }, };
            var entry2 = new IndexEntry { Id = "entry/2", Content = new string[] { "road", "mountain" }, };

            IndexEntries(new[] { entry1, entry2 });

            {
                Span<long> ids = stackalloc long[16];

                using var searcher = new IndexSearcher(Env);
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
            var entry1 = new IndexEntry { Id = "entry/1", Content = new string[] { "road", "lake" }, };
            var entry2 = new IndexEntry { Id = "entry/2", Content = new string[] { "road", "mountain" }, };

            IndexEntries(new[] { entry1, entry2 });

            {
                using var searcher = new IndexSearcher(Env);
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
            var entry1 = new IndexEntry { Id = "entry/1", Content = new string[] { "road", "lake" }, };
            var entry2 = new IndexEntry { Id = "entry/2", Content = new string[] { "road", "mountain" }, };
            var entry3 = new IndexEntry { Id = "entry/3", Content = new string[] { "trail", "mountain" }, };

            IndexEntries(new[] { entry1, entry2, entry3 });

            {
                using var searcher = new IndexSearcher(Env);
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
            var entry1 = new IndexEntry { Id = "entry/1", Content = new string[] { "road", "lake", "mountain" }, };
            var entry2 = new IndexEntry { Id = "entry/2", Content = new string[] { "road", "mountain" }, };
            var entry3 = new IndexEntry { Id = "entry/3", Content = new string[] { "sky", "space" }, };

            IndexEntries(new[] { entry1, entry2, entry3 });

            {
                using var searcher = new IndexSearcher(Env);
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
                using var searcher = new IndexSearcher(Env);
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
        [InlineData(new object[] { 100000, 128 })]
        [InlineData(new object[] { 100000, 18 })]
        [InlineData(new object[] { 1000, 8 })]
        public void SimpleAndOrForBiggerSet(int setSize, int stackSize)
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
                        0 => new string[] { "road", "lake", "mountain" },
                        1 => new string[] { "road", "mountain" },
                        2 => new string[] { "sky", "space", "lake" },
                    }
                };

                entriesToIndex[i] = entry;
            }

            IndexEntries(entriesToIndex);

            {
                using var searcher = new IndexSearcher(Env);
                var match1 = searcher.TermQuery("Content", "lake");
                var match2 = searcher.TermQuery("Content", "mountain");
                var andMatch = searcher.And(in match1, in match2);
                var match3 = searcher.TermQuery("Content", "space");
                var orMatch = searcher.Or(in andMatch, in match3);

                Span<long> ids = stackalloc long[stackSize];
                int read;
                int count = 0;
                do
                {
                    read = orMatch.Fill(ids);
                    count += read;
                } while (read != 0);

                Assert.Equal((setSize / 3) * 2, count);
            }
        }

        [Fact]
        public void SimpleInStatement()
        {
            var entry1 = new IndexEntry { Id = "entry/1", Content = new string[] { "road", "lake", "mountain" }, };
            var entry2 = new IndexEntry { Id = "entry/2", Content = new string[] { "road", "mountain" }, };
            var entry3 = new IndexEntry { Id = "entry/3", Content = new string[] { "sky", "space" }, };

            IndexEntries(new[] { entry1, entry2, entry3 });

            using var searcher = new IndexSearcher(Env);
            {
                var match = searcher.InQuery("Content", new() { "road", "space" });

                Span<long> ids = stackalloc long[2];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.InQuery("Content", new() { "road", "space" });

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.InQuery("Content", new() { "sky", "space" });

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match = searcher.InQuery("Content", new() { "road", "mountain", "space" });

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }
        }

        [Theory]
        [InlineData(new object[] { 100000, 128 })]
        [InlineData(new object[] { 100000, 2046 })]
        [InlineData(new object[] { 1000, 8 })]
        [InlineData(new object[] { 11700, 18 })]
        [InlineData(new object[] { 11859, 18 })]
        public void AndInStatement(int setSize, int stackSize)
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
                        0 => new string[] { "road", "lake", "mountain" },
                        1 => new string[] { "road", "mountain" },
                        2 => new string[] { "sky", "space", "lake" },
                    }
                };

                entriesToIndex[i] = entry;
            }

            IndexEntries(entriesToIndex);

            using var searcher = new IndexSearcher(Env);
            {
                var match1 = searcher.InQuery("Content", new() { "lake", "mountain" });
                var match2 = searcher.TermQuery("Content", "sky");
                var andMatch = searcher.And(in match1, in match2);

                Span<long> ids = stackalloc long[stackSize];
                int read;
                int count = 0;
                do
                {
                    read = andMatch.Fill(ids);
                    count += read;
                } while (read != 0);

                Assert.Equal((setSize / 3), count);
            }

            {
                var match1 = searcher.TermQuery("Content", "sky");
                var match2 = searcher.InQuery("Content", new() { "lake", "mountain" });
                var andMatch = searcher.And(in match1, in match2);

                Span<long> ids = stackalloc long[stackSize];
                int read;
                int count = 0;
                do
                {
                    read = andMatch.Fill(ids);
                    count += read;
                } while (read != 0);

                Assert.Equal((setSize / 3), count);
            }
        }

        [Fact]
        public void SimpleStartWithStatement()
        {
            var entry1 = new IndexEntry { Id = "entry/1", Content = new string[] { "a road", "a lake", "the mountain" }, };
            var entry2 = new IndexEntry { Id = "entry/2", Content = new string[] { "a road", "the mountain" }, };
            var entry3 = new IndexEntry { Id = "entry/3", Content = new string[] { "the sky", "the space", "an animal" }, };

            IndexEntries(new[] { entry1, entry2, entry3 });

            using var searcher = new IndexSearcher(Env);
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
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }
        }

        private static Span<byte> CreateIndexEntry(ref IndexEntryWriter entryWriter, IndexSingleEntry value)
        {
            Span<byte> PrepareString(string value)
            {
                if (value == null)
                    return Span<byte>.Empty;
                return Encoding.UTF8.GetBytes(value);
            }

            entryWriter.Write(IdIndex, PrepareString(value.Id));
            entryWriter.Write(ContentIndex, PrepareString(value.Content));

            entryWriter.Finish(out var output);
            return output;
        }

        private void IndexEntries(IEnumerable<IndexSingleEntry> list)
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            Dictionary<Slice, int> knownFields = CreateKnownFields(bsc);

            const int bufferSize = 4096;
            using var _ = bsc.Allocate(bufferSize, out ByteString buffer);

            {
                using var indexWriter = new IndexWriter(Env);
                foreach (var entry in list)
                {
                    var entryWriter = new IndexEntryWriter(buffer.ToSpan(), knownFields);
                    var data = CreateIndexEntry(ref entryWriter, entry);
                    indexWriter.Index(entry.Id, data, knownFields);
                }

                indexWriter.Commit();
            }
        }

        [Fact]
        public void MixedSortedMatchStatement()
        {
            var entry1 = new IndexSingleEntry { Id = "entry/1", Content = "3" };
            var entry2 = new IndexEntry { Id = "entry/2", Content = new string[] { "4", "2" }, };
            var entry3 = new IndexSingleEntry { Id = "entry/3", Content = "1" };

            IndexEntries(new[] { entry1, entry3 });
            IndexEntries(new[] { entry2 });

            using var searcher = new IndexSearcher(Env);
            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.OrderByAscending(match1, ContentIndex);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }
        }


        [Fact]
        public void WillGetTotalNumberOfResultsInPagedQuery()
        {
            var entry1 = new IndexSingleEntry { Id = "entry/1", Content = "3" };
            var entry2 = new IndexEntry { Id = "entry/2", Content = new string[] { "4", "2" }, };
            var entry3 = new IndexSingleEntry { Id = "entry/3", Content = "1" };

            IndexEntries(new[] { entry1, entry3 });
            IndexEntries(new[] { entry2 });

            using var searcher = new IndexSearcher(Env);
            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.OrderByAscending(match1, ContentIndex, take: 2);

                Span<long> ids = stackalloc long[2];
                Assert.Equal(2, match.Fill(ids));
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
                list.Add(new IndexSingleEntry() { Id = $"entry/{i + 1}", Content = i.ToString() });
            }

            IndexEntries(list);
            IndexEntries(new[] { new IndexEntry() { Id = $"entry/{i + 1}" } });
            list.Add(new IndexSingleEntry() { Id = $"entry/{i + 1}" });

            using var searcher = new IndexSearcher(Env);
            {
                var all = searcher.AllEntries();
                var results = new List<string>();
                int read;
                Span<long> ids = stackalloc long[256];
                while ((read = all.Fill(ids)) != 0)
                {
                    for (i = 0; i < read; ++i)
                        results.Add(searcher.GetIdentityFor(ids[i]));
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
            var entry1 = new IndexSingleEntry { Id = "entry/1", Content = "3" };
            var entry2 = new IndexSingleEntry { Id = "entry/2", Content = "2" };
            var entry3 = new IndexSingleEntry { Id = "entry/3", Content = "1" };

            IndexEntries(new[] { entry1, entry2, entry3 });

            using var searcher = new IndexSearcher(Env);

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.OrderByAscending(match1, ContentIndex);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.OrderByAscending(match1, ContentIndex);

                Span<long> ids = stackalloc long[2];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));

                Assert.Equal("entry/3", searcher.GetIdentityFor(ids[0]));
            }
        }


        private void IndexEntriesDouble(IEnumerable<IndexSingleEntryDouble> list)
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            Dictionary<Slice, int> knownFields = CreateKnownFields(bsc);

            const int bufferSize = 4096;
            using var _ = bsc.Allocate(bufferSize, out ByteString buffer);

            {
                using var indexWriter = new IndexWriter(Env);
                foreach (var entry in list)
                {
                    var entryWriter = new IndexEntryWriter(buffer.ToSpan(), knownFields);
                    var data = CreateIndexEntryDouble(ref entryWriter, entry);
                    indexWriter.Index(entry.Id, data, knownFields);
                }

                indexWriter.Commit();
            }
        }

        private static Span<byte> CreateIndexEntryDouble(ref IndexEntryWriter entryWriter, IndexSingleEntryDouble value)
        {
            Span<byte> PrepareString(string value)
            {
                if (value == null)
                    return Span<byte>.Empty;
                return Encoding.UTF8.GetBytes(value);
            }

            entryWriter.Write(IdIndex, PrepareString(value.Id));
            entryWriter.Write(ContentIndex, PrepareString(value.Content.ToString()), Convert.ToInt64(value.Content), value.Content);

            entryWriter.Finish(out var output);
            return output;
        }

        private class IndexSingleEntryDouble
        {
            public string Id;
            public double Content;
        }

        [Fact]
        public void SimpleOrdinalCompareStatementWithLongValue()
        {
            var list = new List<IndexSingleEntryDouble>();
            for (int i = 1; i < 1001; ++i)
                list.Add(new IndexSingleEntryDouble() { Id = $"entry/{i}", Content = (double)i });
            List<string> qids = new();
            IndexEntriesDouble(list);
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env);
            Span<long> ids = stackalloc long[1024];

            {
                var match1 = searcher.AllEntries();
                var match2 = searcher.LessThan(match1, ContentIndex, 25D);
                
                int read = 0;
                while ((read = match2.Fill(ids)) != 0)
                    while (--read >= 0)
                        qids.Add(searcher.GetIdentityFor(ids[read]));

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
                var matchBetween = searcher.Between(searcher.AllEntries(), ContentIndex, 100L, 200L);
                var read = matchBetween.Fill(ids);
                while (--read >= 0)
                {
                    qids.Add(searcher.GetIdentityFor(ids[read]));
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

            qids.Clear();
            {
                var matchBetween = searcher.NotBetween(searcher.AllEntries(), ContentIndex, 100L, 200L);
                var read = matchBetween.Fill(ids);
                while (--read >= 0)
                {
                    qids.Add(searcher.GetIdentityFor(ids[read]));
                }

                foreach (IndexSingleEntryDouble indexSingleEntryDouble in list)
                {
                    bool isIn = qids.Contains(indexSingleEntryDouble.Id);
                    if (indexSingleEntryDouble.Content is < 100D or > 200D)
                        Assert.True(isIn);
                    else
                        Assert.False(isIn);
                }
            }
        }


        [Fact]
        public void SimpleOrdinalCompareStatement()
        {
            var entry1 = new IndexSingleEntry { Id = "entry/1", Content = "3" };
            var entry2 = new IndexSingleEntry { Id = "entry/2", Content = "2" };
            var entry3 = new IndexSingleEntry { Id = "entry/3", Content = "1" };

            IndexEntries(new[] { entry1, entry2, entry3 });

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env);

            Slice.From(bsc, "1", out var one);

            {
                //   var match1 = searcher.StartWithQuery("Id", "e");
                var match1 = searcher.AllEntries();
                var match = searcher.GreaterThan(match1, ContentIndex, one);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.GreaterThanOrEqual(match1, ContentIndex, one);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.LessThan(match1, ContentIndex, one);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.LessThanOrEqual(match1, ContentIndex, one);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }
        }


        [Fact]
        public void SimpleEqualityCompareStatement()
        {
            var entry1 = new IndexSingleEntry { Id = "entry/1", Content = "1" };
            var entry2 = new IndexSingleEntry { Id = "entry/2", Content = "2" };
            var entry3 = new IndexSingleEntry { Id = "entry/3", Content = "1" };

            IndexEntries(new[] { entry1, entry2, entry3 });

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env);

            Slice.From(bsc, "1", out var one);
            Slice.From(bsc, "4", out var four);

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.Equals(match1, ContentIndex, one);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.NotEquals(match1, ContentIndex, one);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.Equals(match1, ContentIndex, four);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.NotEquals(match1, ContentIndex, four);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }
        }

        [Fact]
        public void SimpleBetweenCompareStatement()
        {
            var entry1 = new IndexSingleEntry { Id = "entry/1", Content = "3" };
            var entry2 = new IndexSingleEntry { Id = "entry/2", Content = "2" };
            var entry3 = new IndexSingleEntry { Id = "entry/3", Content = "1" };
            var entry4 = new IndexSingleEntry { Id = "entry/4", Content = "4" };

            IndexEntries(new[] { entry1, entry2, entry3, entry4 });

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env);

            Slice.From(bsc, "0", out var zero);
            Slice.From(bsc, "1", out var one);
            Slice.From(bsc, "2", out var two);
            Slice.From(bsc, "3", out var three);
            Slice.From(bsc, "4", out var four);

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.Between(match1, ContentIndex, one, two);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.Between(match1, ContentIndex, zero, three);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.Between(match1, ContentIndex, zero, zero);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.Between(match1, ContentIndex, one, one);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }
        }

        [Fact]
        public void SimpleNotBetweenCompareStatement()
        {
            var entry1 = new IndexSingleEntry { Id = "entry/1", Content = "3" };
            var entry2 = new IndexSingleEntry { Id = "entry/2", Content = "2" };
            var entry3 = new IndexSingleEntry { Id = "entry/3", Content = "1" };
            var entry4 = new IndexSingleEntry { Id = "entry/4", Content = "4" };

            IndexEntries(new[] { entry1, entry2, entry3, entry4 });

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            using var searcher = new IndexSearcher(Env);

            Slice.From(bsc, "0", out var zero);
            Slice.From(bsc, "1", out var one); //
            Slice.From(bsc, "2", out var two); //
            Slice.From(bsc, "3", out var three); //
            Slice.From(bsc, "4", out var four); //
            Slice.From(bsc, "5", out var five);

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.NotBetween(match1, ContentIndex, two, three);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(2, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.NotBetween(match1, ContentIndex, two, two);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(3, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.NotBetween(match1, ContentIndex, one, four);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(0, match.Fill(ids));
            }

            {
                var match1 = searcher.StartWithQuery("Id", "e");
                var match = searcher.NotBetween(match1, ContentIndex, zero, three);

                Span<long> ids = stackalloc long[16];
                Assert.Equal(1, match.Fill(ids));
                Assert.Equal(0, match.Fill(ids));
            }
        }

        [Theory]
        [InlineData(new object[] { 100000, 128 })]
        [InlineData(new object[] { 100000, 2046 })]
        [InlineData(new object[] { 1000, 8 })]
        [InlineData(new object[] { 11700, 18 })]
        [InlineData(new object[] { 11859, 18 })]
        public void AndInStatementWithLowercaseAnalyzer(int setSize, int stackSize)
        {
            var analyzer = Analyzer.Create<KeywordTokenizer, LowerCaseTransformer>();

            setSize = setSize - (setSize % 3);

            var entriesToIndex = new IndexEntry[setSize];
            for (int i = 0; i < setSize; i++)
            {
                var entry = new IndexEntry
                {
                    Id = $"entry/{i}",
                    Content = (i % 3) switch
                    {
                        0 => new string[] { "road", "Lake", "mounTain" },
                        1 => new string[] { "roAd", "mountain" },
                        2 => new string[] { "sky", "space", "laKe" },
                    }
                };

                entriesToIndex[i] = entry;
            }

            IndexEntries(entriesToIndex, analyzer);

            using var searcher = new IndexSearcher(Env);
            {
                var match1 = searcher.InQuery("Content", new() { "lake", "mountain" });
                var match2 = searcher.TermQuery("Content", "sky");
                var andMatch = searcher.And(in match1, in match2);

                Span<long> ids = stackalloc long[stackSize];
                int read;
                int count = 0;
                do
                {
                    read = andMatch.Fill(ids);
                    count += read;
                } while (read != 0);

                Assert.Equal((setSize / 3), count);
            }

            {
                var match1 = searcher.TermQuery("Content", "sky");
                var match2 = searcher.InQuery("Content", new() { "lake", "mountain" });
                var andMatch = searcher.And(in match1, in match2);

                Span<long> ids = stackalloc long[stackSize];
                int read;
                int count = 0;
                do
                {
                    read = andMatch.Fill(ids);
                    count += read;
                } while (read != 0);

                Assert.Equal((setSize / 3), count);
            }
        }

        [Theory]
        [InlineData(new object[] { 100000, 128 })]
        [InlineData(new object[] { 100000, 2046 })]
        [InlineData(new object[] { 1000, 8 })]
        [InlineData(new object[] { 11700, 18 })]
        [InlineData(new object[] { 11859, 18 })]
        public void AndInStatementAndWhitespaceTokenizer(int setSize, int stackSize)
        {
            var analyzer = Analyzer.Create<WhitespaceTokenizer, LowerCaseTransformer>();

            setSize = setSize - (setSize % 3);

            var entriesToIndex = new IndexEntry[setSize];
            for (int i = 0; i < setSize; i++)
            {
                var entry = new IndexEntry
                {
                    Id = $"entry/{i}",
                    Content = (i % 3) switch
                    {
                        0 => new string[] { "road Lake mounTain  " },
                        1 => new string[] { "roAd mountain" },
                        2 => new string[] { "sky space laKe" },
                    }
                };

                entriesToIndex[i] = entry;
            }

            IndexEntries(entriesToIndex, analyzer);

            using var searcher = new IndexSearcher(Env);
            {
                var match1 = searcher.InQuery("Content", new() { "lake", "mountain" });
                var match2 = searcher.TermQuery("Content", "sky");
                var andMatch = searcher.And(in match1, in match2);

                Span<long> ids = stackalloc long[stackSize];
                int read;
                int count = 0;
                do
                {
                    read = andMatch.Fill(ids);
                    count += read;
                } while (read != 0);

                Assert.Equal((setSize / 3), count);
            }

            {
                var match1 = searcher.TermQuery("Content", "sky");
                var match2 = searcher.InQuery("Content", new() { "lake", "mountain" });
                var andMatch = searcher.And(in match1, in match2);

                Span<long> ids = stackalloc long[stackSize];
                int read;
                int count = 0;
                do
                {
                    read = andMatch.Fill(ids);
                    count += read;
                } while (read != 0);

                Assert.Equal((setSize / 3), count);
            }
        }
    }
}
