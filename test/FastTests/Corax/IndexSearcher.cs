using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
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
                var id = $"entry/1";
                var entryWriter = new IndexEntryWriter(buffer.ToSpan(), knownFields);

                using var indexWriter = new IndexWriter(Env);
                foreach (var entry in list)
                {
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
                Assert.Equal(0, match.TotalResults);
                Assert.False(match.MoveNext(out var _));
                Assert.Equal(QueryMatch.Invalid, match.Current);

                match = searcher.TermQuery("Id", "1");
                Assert.Equal(0, match.TotalResults);
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
                Assert.NotEqual(QueryMatch.Invalid, match.Current);
                Assert.Equal(1, match.TotalResults);
                Assert.False(match.MoveNext(out var _));
                Assert.Equal(QueryMatch.Invalid, match.Current);
            }
        }
    }
}
