using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Corax;
using Corax.Pipeline;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax
{
    public class SuggestionsTests : StorageTest
    {
        private const int IndexId = 0, ContentId = 1;

        public SuggestionsTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public unsafe void BasicIndexing()
        {
            var entry1 = new IndexEntryValues { Id = "entry/1", Content = "road lake" };
            var entry2 = new IndexEntryValues { Id = "entry/2", Content = "road mountain" };
            var entry3 = new IndexEntryValues { Id = "entry/3", Content = "mountain roadshow" };

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

            var mapping = CreateKnownFields(bsc);

            IndexEntries(bsc, new[] { entry1, entry2, entry3 }, mapping);

            {
                using var searcher = new IndexSearcher(Env);

                var match = searcher.Suggest(ContentId, "road");

                Span<byte> terms = stackalloc byte[1024];
                Span<Token> tokens = stackalloc Token[16];

                match.Next(ref terms, ref tokens);
                Assert.Equal(3, tokens.Length);
            }
        }

        [Fact]
        public unsafe void WithRemoves()
        {
            var entry1 = new IndexEntryValues { Id = "entry/1", Content = "road lake" };
            var entry2 = new IndexEntryValues { Id = "entry/2", Content = "road mountain" };
            var entry3 = new IndexEntryValues { Id = "entry/3", Content = "mountain roadshow" };

            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

            var mapping = CreateKnownFields(bsc);

            IndexEntries(bsc, new[] { entry1, entry2, entry3 }, mapping);

            {
                using var indexWriter = new IndexWriter(Env, mapping);
                indexWriter.TryDeleteEntry("Id", entry1.Id);
                indexWriter.Commit();
            }

            {
                using var searcher = new IndexSearcher(Env);

                var match = searcher.Suggest(ContentId, "road");

                Span<byte> terms = stackalloc byte[1024];
                Span<Token> tokens = stackalloc Token[16];

                match.Next(ref terms, ref tokens);
                Assert.Equal(2, tokens.Length);
            }
        }

        public const int IdIndex = 0,
                         ContentIndex = 1;

        private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx, Analyzer analyzer = null)
        {
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            return new IndexFieldsMapping(ctx)
                .AddBinding(IdIndex, idSlice, analyzer)
                .AddBinding(ContentIndex, contentSlice, analyzer, hasSuggestion:true);
            ;
        }

        private static Span<byte> CreateIndexEntry(ref IndexEntryWriter entryWriter, IndexEntryValues value)
        {
            Span<byte> PrepareString(string value)
            {
                if (value == null)
                    return Span<byte>.Empty;
                return Encoding.UTF8.GetBytes(value);
            }

            entryWriter.Write(IndexId, PrepareString(value.Id));
            entryWriter.Write(ContentId, PrepareString(value.Content));

            entryWriter.Finish(out var output);
            return output;
        }

        private void IndexEntries(ByteStringContext bsc, IEnumerable<IndexEntryValues> list, IndexFieldsMapping mapping)
        {
            const int bufferSize = 4096;
            using var _ = bsc.Allocate(bufferSize, out ByteString buffer);

            {
                using var indexWriter = new IndexWriter(Env, mapping);
                foreach (var entry in list)
                {
                    var entryWriter = new IndexEntryWriter(buffer.ToSpan(), mapping);
                    var data = CreateIndexEntry(ref entryWriter, entry);
                    indexWriter.Index(entry.Id, data, mapping);
                }

                indexWriter.Commit();
            }
        }

        private class IndexEntryValues
        {
            public string Id { get; set; }
            public string Content { get; set; }
        }
    }
}
