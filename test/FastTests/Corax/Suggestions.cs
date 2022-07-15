using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Corax;
using Corax.Analyzers;
using Corax.IndexEntry;
using Corax.Pipeline;
using Corax.Queries;
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
        public unsafe void NGramBasicIndexing()
        {
            var entry1 = new IndexEntryValues { Id = "entry/1", Content = "road lake" };
            var entry2 = new IndexEntryValues { Id = "entry/2", Content = "road mountain" };
            var entry3 = new IndexEntryValues { Id = "entry/3", Content = "mountain roadshow" };
            var entry4 = new IndexEntryValues { Id = "entry/4", Content = "hello world" };
            var entry5 = new IndexEntryValues { Id = "entry/5", Content = "ravendbissuper" };


            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

            var mapping = CreateKnownFields(bsc);

            IndexEntries(bsc, new[] { entry1, entry2, entry3, entry4, entry5 }, mapping);

            {
                using var searcher = new IndexSearcher(Env, mapping);

                var match = searcher.Suggest(ContentId, "road", false, StringDistanceAlgorithm.None, 0);

                Span<byte> terms = stackalloc byte[1024];
                Span<Token> tokens = stackalloc Token[16];
                Span<float> scores = stackalloc float[16];
                match.Next(ref terms, ref tokens, ref scores);

                var ngrams = new HashSet<string>();
                foreach (var token in tokens)
                {
                    var term = terms.Slice(token.Offset, (int)token.Length);
                    var asString = Encoding.UTF8.GetString(term);
                    ngrams.Add(asString);
                }

                Assert.Equal(3, ngrams.Count);
                Assert.Equal(tokens.Length, scores.Length);
            }

            {
                using var searcher = new IndexSearcher(Env, mapping);

                var match = searcher.Suggest(ContentId, "road", false, StringDistanceAlgorithm.NGram, 0.35f);

                Span<byte> terms = stackalloc byte[1024];
                Span<Token> tokens = stackalloc Token[16];
                Span<float> scores = stackalloc float[16];
                match.Next(ref terms, ref tokens, ref scores);

                var ngrams = new HashSet<string>();
                foreach (var token in tokens)
                {
                    var term = terms.Slice(token.Offset, (int)token.Length);
                    var asString = Encoding.UTF8.GetString(term);
                    ngrams.Add(asString);
                }

                Assert.Equal(1, ngrams.Count);
                Assert.Equal(tokens.Length, scores.Length);
            }

        }

        [Fact]
        public unsafe void NGramWithRemoves()
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
                using var searcher = new IndexSearcher(Env, mapping);

                var match = searcher.Suggest(ContentId, "road", false, StringDistanceAlgorithm.None, 0f);

                Span<byte> terms = stackalloc byte[1024];
                Span<Token> tokens = stackalloc Token[16];
                Span<float> scores = stackalloc float[16];

                match.Next(ref terms, ref tokens, ref scores);
                Assert.Equal(2, tokens.Length);
                Assert.Equal(tokens.Length, scores.Length);
            }
        }

        [Fact]
        public unsafe void LevenshteinBasicIndexing()
        {
            var entry1 = new IndexEntryValues { Id = "entry/1", Content = "road lake" };
            var entry2 = new IndexEntryValues { Id = "entry/2", Content = "road mountain" };
            var entry3 = new IndexEntryValues { Id = "entry/3", Content = "mountain roadshow" };
            var entry4 = new IndexEntryValues { Id = "entry/4", Content = "hello world" };
            var entry5 = new IndexEntryValues { Id = "entry/5", Content = "ravendbissuper" };


            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

            var mapping = CreateKnownFields(bsc, Analyzer.DefaultAnalyzer);

            IndexEntries(bsc, new[] { entry1, entry2, entry3, entry4, entry5 }, mapping);
            {
                using var searcher = new IndexSearcher(Env, mapping);

                var match = searcher.Suggest(ContentId, "road lakz", true, StringDistanceAlgorithm.Levenshtein, 0.5f);

                Span<byte> terms = stackalloc byte[1024];
                Span<Token> tokens = stackalloc Token[16];
                Span<float> scores = stackalloc float[16];

                match.Next(ref terms, ref tokens, ref scores);
                Assert.Equal(1, tokens.Length);
                Assert.Equal("road lake", Encoding.UTF8.GetString(terms));
                Assert.Equal(tokens.Length, scores.Length);
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
                    indexWriter.Index(entry.Id, data);
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
