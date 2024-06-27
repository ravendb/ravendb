using System;
using System.Collections.Generic;
using System.Text;
using Corax;
using Corax.Analyzers;
using Corax.Querying;
using Corax.Mappings;
using Corax.Pipeline;
using FastTests.Voron;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;
using Xunit.Abstractions;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

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

            using var mapping = CreateKnownFields(bsc);

            IndexEntries(bsc, new[] { entry1, entry2, entry3, entry4, entry5 }, mapping);

            {
                using var searcher = new IndexSearcher(Env, mapping);

                var match = searcher.Suggest(searcher.FieldMetadataBuilder("Content", 1), "road", false, StringDistanceAlgorithm.None, 0);

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

                var match = searcher.Suggest(searcher.FieldMetadataBuilder("Content", 1), "road", false, StringDistanceAlgorithm.NGram, 0.35f);

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

            using var mapping = CreateKnownFields(bsc);

            IndexEntries(bsc, new[] { entry1, entry2, entry3 }, mapping);
            mapping.TryGetByFieldId(1, out var contentField);

            {
                using var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All);
                indexWriter.TryDeleteEntry(entry1.Id);
                indexWriter.Commit();
            }

            {
                using var searcher = new IndexSearcher(Env, mapping);

                var match = searcher.Suggest(contentField.Metadata, "road", false, StringDistanceAlgorithm.None, 0f);

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

            using var mapping = CreateKnownFields(bsc, Analyzer.CreateDefaultAnalyzer(bsc));
            mapping.TryGetByFieldId(1, out var contentField);

            IndexEntries(bsc, new[] { entry1, entry2, entry3, entry4, entry5 }, mapping);
            {
                using var searcher = new IndexSearcher(Env, mapping);
                searcher.FieldMetadataBuilder("Content", 1);
                var match = searcher.Suggest(contentField.Metadata, "road lakz", true, StringDistanceAlgorithm.Levenshtein, 0.5f);

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

            using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
                .AddBinding(IdIndex, idSlice, analyzer)
                .AddBinding(ContentIndex, contentSlice, analyzer, hasSuggestion: true);
            return builder.Build();
        }

      
        private static void CreateEntry(IndexWriter indexWriter, IndexEntryValues entry)
        {
            using var builder = indexWriter.Index(entry.Id);
            builder.Write(IndexId, PrepareString(entry.Id));
            builder.Write(ContentId, PrepareString(entry.Content));
            Span<byte> PrepareString(string value)
            {
                if (value == null)
                    return Span<byte>.Empty;
                return Encoding.UTF8.GetBytes(value);
            }

        }

        private void IndexEntries(ByteStringContext bsc, IEnumerable<IndexEntryValues> list, IndexFieldsMapping mapping)
        {
            using var indexWriter = new IndexWriter(Env, mapping, SupportedFeatures.All);

            foreach (var entry in list)
            {
                CreateEntry(indexWriter, entry);
            }

            indexWriter.Commit();
        }

        private class IndexEntryValues
        {
            public string Id { get; set; }
            public string Content { get; set; }
        }
    }
}
