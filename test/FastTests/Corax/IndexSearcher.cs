using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using Corax.Queries;
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

            return new Dictionary<Slice, int>
            {
                [idSlice] = IdIndex,
                [contentSlice] = ContentIndex,
            };
        }



        public IndexSearcherTest(ITestOutputHelper output) : base(output)
        {
        }


        private void IndexEntries(IEnumerable<IndexEntry> list)
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
        public void EmptyTerm()
        {
            var entry = new IndexEntry
            {
                Id = "entry/1",
                Content = new string[] { "road", "lake" },
            };
            IndexEntries(new[] { entry });

            {
                using var searcher = new IndexSearcher(Env);
                var match = searcher.TermQuery("Unknown", "1");
                Assert.Equal(QueryMatch.Start, match.Current);
                Assert.Equal(0, match.Count);                
                Assert.False(match.MoveNext(out var _));
                Assert.Equal(QueryMatch.Invalid, match.Current);

                match = searcher.TermQuery("Id", "1");
                Assert.Equal(QueryMatch.Start, match.Current);
                Assert.Equal(0, match.Count);
                Assert.False(match.MoveNext(out var _));
                Assert.Equal(QueryMatch.Invalid, match.Current);
            }
        }

        [Fact]
        public void SingleTerm()
        {
            var entry = new IndexEntry
            {
                Id = "entry/1",
                Content = new string[] { "road", "lake" },
            };
            IndexEntries(new[] { entry });

            {
                using var searcher = new IndexSearcher(Env);
                var match = searcher.TermQuery("Id", "entry/1");
                Assert.Equal(QueryMatch.Start, match.Current);
                Assert.Equal(1, match.Count);
                Assert.True(match.MoveNext(out var _));
                Assert.NotEqual(QueryMatch.Invalid, match.Current);
                Assert.False(match.MoveNext(out var _));
                Assert.Equal(QueryMatch.Invalid, match.Current);
            }
        }

        [Fact]
        public void SmallSetTerm()
        {
            var entries = new IndexEntry[16];
            for (int i = 0; i < entries.Length; i++)
            {
                entries[i] = new IndexEntry
                {
                    Id = $"entry/{i}",
                    Content = new string[] { "road" },
                };
            }
            IndexEntries(entries);

            {
                using var searcher = new IndexSearcher(Env);
                var match = searcher.TermQuery("Content", "road");

                Assert.Equal(16, match.Count);

                // TODO: For consistency this should be true. 
                // Assert.Equal(QueryMatch.Start, match.Current);

                int i = 0;
                while (match.MoveNext(out var v))
                {
                    i++;

                    Assert.Equal(v, match.Current);
                    Assert.NotEqual(QueryMatch.Invalid, match.Current);
                    Assert.NotEqual(QueryMatch.Start, match.Current);
                }

                Assert.Equal(i, match.Count);
                Assert.Equal(QueryMatch.Invalid, match.Current);
            }
        }

        [Fact]
        public void SetTerm()
        {
            var entries = new IndexEntry[100000];
            var content = new string[] { "road" };

            for (int i = 0; i < entries.Length; i++)
            {
                entries[i] = new IndexEntry
                {
                    Id = $"entry/{i}",
                    Content = content,
                };
            }
            IndexEntries(entries);

            {
                using var searcher = new IndexSearcher(Env);
                var match = searcher.TermQuery("Content", "road");

                Assert.Equal(100000, match.Count);
                Assert.Equal(QueryMatch.Start, match.Current);

                int i = 0;
                while (match.MoveNext(out var v))
                {
                    i++;

                    Assert.Equal(v, match.Current);
                    Assert.NotEqual(QueryMatch.Invalid, match.Current);
                    Assert.NotEqual(QueryMatch.Start, match.Current);
                }

                Assert.Equal(i, match.Count);
                Assert.Equal(QueryMatch.Invalid, match.Current);

                Assert.True(match.SeekTo(QueryMatch.Start));
                
                int j = 0;
                while (match.MoveNext(out var v))
                {
                    j++;

                    Assert.Equal(v, match.Current);
                    Assert.NotEqual(QueryMatch.Invalid, match.Current);
                    Assert.NotEqual(QueryMatch.Start, match.Current);
                }

                Assert.Equal(j, i);
            }
        }


        [Fact]
        public void EmptyAnd()
        {
            var entry1 = new IndexEntry
            {
                Id = "entry/1",
                Content = new string[] { "road", "lake" },
            };
            var entry2 = new IndexEntry
            {
                Id = "entry/2",
                Content = new string[] { "road", "mountain" },
            };

            IndexEntries(new[] { entry1, entry2 });

            {
                using var searcher = new IndexSearcher(Env);
                var match1 = searcher.TermQuery("Id", "entry/1");
                var match2 = searcher.TermQuery("Content", "mountain");
                var andMatch = searcher.And(in match1, in match2);

                Assert.Equal(QueryMatch.Start, andMatch.Current);

                int i = 0;
                while(andMatch.MoveNext(out var _))
                    i++;

                Assert.Equal(QueryMatch.Invalid, andMatch.Current);
                Assert.Equal(0, i);
            }
        }

        [Fact]
        public void SingleAnd()
        {
            var entry1 = new IndexEntry
            {
                Id = "entry/1",
                Content = new string[] { "road", "lake" },
            };
            var entry2 = new IndexEntry
            {
                Id = "entry/1",
                Content = new string[] { "road", "mountain" },
            };

            IndexEntries(new[] { entry1, entry2 });

            {
                using var searcher = new IndexSearcher(Env);
                var match1 = searcher.TermQuery("Id", "entry/1");
                var match2 = searcher.TermQuery("Content", "mountain");
                var andMatch = searcher.And(in match1, in match2);

                Assert.Equal(QueryMatch.Start, andMatch.Current);

                int i = 0;
                while (andMatch.MoveNext(out var _))
                    i++;

                Assert.Equal(QueryMatch.Invalid, andMatch.Current);
                Assert.Equal(1, i);
            }
        }

        [Fact]
        public void AllAnd()
        {
            var entry1 = new IndexEntry
            {
                Id = "entry/1",
                Content = new string[] { "road", "lake", "mountain" },
            };
            var entry2 = new IndexEntry
            {
                Id = "entry/1",
                Content = new string[] { "road", "mountain" },
            };

            IndexEntries(new[] { entry1, entry2 });

            {
                using var searcher = new IndexSearcher(Env);
                var match1 = searcher.TermQuery("Id", "entry/1");
                var match2 = searcher.TermQuery("Content", "mountain");
                var andMatch = searcher.And(in match1, in match2);

                Assert.Equal(QueryMatch.Start, andMatch.Current);

                int i = 0;
                while (andMatch.MoveNext(out var _))
                    i++;

                Assert.Equal(QueryMatch.Invalid, andMatch.Current);
                Assert.Equal(2, i);
            }
        }

        [Fact]
        public void EmptyOr()
        {
            var entry1 = new IndexEntry
            {
                Id = "entry/1",
                Content = new string[] { "road", "lake" },
            };
            var entry2 = new IndexEntry
            {
                Id = "entry/2",
                Content = new string[] { "road", "mountain" },
            };

            IndexEntries(new[] { entry1, entry2 });

            {
                using var searcher = new IndexSearcher(Env);
                var match1 = searcher.TermQuery("Id", "entry/3");
                var match2 = searcher.TermQuery("Content", "highway");
                var andMatch = searcher.Or(in match1, in match2);

                Assert.Equal(QueryMatch.Start, andMatch.Current);

                int i = 0;
                while (andMatch.MoveNext(out var _))
                    i++;

                Assert.Equal(QueryMatch.Invalid, andMatch.Current);
                Assert.Equal(0, i);
            }
        }

        [Fact]
        public void SingleOr()
        {
            var entry1 = new IndexEntry
            {
                Id = "entry/1",
                Content = new string[] { "road", "lake" },
            };
            var entry2 = new IndexEntry
            {
                Id = "entry/2",
                Content = new string[] { "road", "mountain" },
            };

            IndexEntries(new[] { entry1, entry2 });

            {
                using var searcher = new IndexSearcher(Env);
                var match1 = searcher.TermQuery("Id", "entry/1");
                var match2 = searcher.TermQuery("Content", "highway");
                var andMatch = searcher.Or(in match1, in match2);

                Assert.Equal(QueryMatch.Start, andMatch.Current);

                int i = 0;
                while (andMatch.MoveNext(out var _))
                    i++;

                Assert.Equal(QueryMatch.Invalid, andMatch.Current);
                Assert.Equal(1, i);

                match1 = searcher.TermQuery("Id", "entry/3");
                match2 = searcher.TermQuery("Content", "mountain");
                andMatch = searcher.Or(in match1, in match2);

                Assert.Equal(QueryMatch.Start, andMatch.Current);

                i = 0;
                while (andMatch.MoveNext(out var _))
                    i++;

                Assert.Equal(QueryMatch.Invalid, andMatch.Current);
                Assert.Equal(1, i);
            }
        }


        [Fact]
        public void AllOr()
        {
            var entry1 = new IndexEntry
            {
                Id = "entry/1",
                Content = new string[] { "road", "lake" },
            };
            var entry2 = new IndexEntry
            {
                Id = "entry/2",
                Content = new string[] { "road", "mountain" },
            };

            IndexEntries(new[] { entry1, entry2 });

            {
                using var searcher = new IndexSearcher(Env);
                var match1 = searcher.TermQuery("Id", "entry/1");
                var match2 = searcher.TermQuery("Content", "mountain");
                var andMatch = searcher.Or(in match1, in match2);

                Assert.Equal(QueryMatch.Start, andMatch.Current);

                int i = 0;
                while (andMatch.MoveNext(out var _))
                    i++;

                Assert.Equal(QueryMatch.Invalid, andMatch.Current);
                Assert.Equal(2, i);
            }
        }

        [Fact]
        public void SimpleAndOr()
        {
            var entry1 = new IndexEntry
            {
                Id = "entry/1",
                Content = new string[] { "road", "lake", "mountain" },
            };
            var entry2 = new IndexEntry
            {
                Id = "entry/2",
                Content = new string[] { "road", "mountain" },
            };
            var entry3 = new IndexEntry
            {
                Id = "entry/3",
                Content = new string[] { "sky", "space" },
            };

            IndexEntries(new[] { entry1, entry2, entry3 });

            {
                using var searcher = new IndexSearcher(Env);
                var match1 = searcher.TermQuery("Id", "entry/1");
                var match2 = searcher.TermQuery("Content", "mountain");
                var andMatch = searcher.And(in match1, in match2);
                var match3 = searcher.TermQuery("Id", "entry/3");
                var orMatch = searcher.Or(in andMatch, in match3);

                Assert.Equal(QueryMatch.Start, orMatch.Current);

                int i = 0;
                while (orMatch.MoveNext(out var _))
                    i++;

                Assert.Equal(QueryMatch.Invalid, orMatch.Current);
                Assert.Equal(2, i);
            }

            {
                using var searcher = new IndexSearcher(Env);
                var match1 = searcher.TermQuery("Id", "entry/1");
                var match2 = searcher.TermQuery("Content", "mountain");
                var andMatch = searcher.And(in match1, in match2);
                var match3 = searcher.TermQuery("Id", "entry/3");
                var orMatch = searcher.Or(in match3, in andMatch);

                Assert.Equal(QueryMatch.Start, orMatch.Current);

                int i = 0;
                while (orMatch.MoveNext(out var _))
                    i++;

                Assert.Equal(QueryMatch.Invalid, orMatch.Current);
                Assert.Equal(2, i);
            }
        }
    }
}
